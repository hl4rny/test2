unit DatabaseHandler;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, DateUtils, Generics.Collections,
  ZConnection, ZDataset, ZDbcIntfs, Dialogs,
  LogHandlers, McJSON, Variants;


const
  DEFAULT_RETENTION_MONTHS = 12;

type
  // 로그 큐 아이템 (비동기 처리용)
  TDBLogQueueItem = record
    Message: string;
    Level: TLogLevel;
    Tag: string;
    Timestamp: TDateTime;  // 타임스탬프 저장 (테이블 선택을 위해)
  end;
  PDBLogQueueItem = ^TDBLogQueueItem;

  // 월별 로그 테이블 정보
  TLogTableInfo = record
    TableName: string;     // 테이블 이름
    YearMonth: string;     // YYYYMM 형식
    CreationDate: TDateTime; // 생성 일자
    LastUpdate: TDateTime;   // 마지막 업데이트
    RecordCount: Integer;    // 대략적인 행 개수 (ROW_COUNT 대신 RecordCount 사용)
  end;

  TLogTableInfoArray = array of TLogTableInfo;

  TLogExtendedData = class
  private
    FJsonObject: TMcJsonObject;
  public
    constructor Create;
    destructor Destroy; override;
    procedure AddProperty(const Name: string; const Value: Variant);
    procedure AddJsonObject(const Name: string; JsonObj: TMcJsonObject);
    procedure AddJsonArray(const Name: string; JsonArr: TMcJsonArray);
    function AsJson: string;
    function AsJsonObject: TMcJsonObject;
  end;


  { TDatabaseHandler - 데이터베이스 로그 핸들러 }
  TDatabaseHandler = class(TLogHandler)
  private
    FConnection: TZConnection;       // 데이터베이스 연결
    FLogQuery: TZQuery;              // 로그 쿼리
    FTablePrefix: string;            // 로그 테이블 접두사 (기본값: LOGS)
    FMetaTableName: string;          // 메타 테이블 이름 (기본값: LOG_META)
    FAutoCreateTable: Boolean;       // 테이블 자동 생성 여부
    FQueueMaxSize: Integer;          // 큐 최대 크기
    FQueueFlushInterval: Integer;    // 큐 자동 플러시 간격
    FLastQueueFlush: TDateTime;      // 마지막 큐 플러시 시간
    FRetentionMonths: Integer;       // 로그 데이터 보관 개월 수
    FLastCleanupDate: TDateTime;     // 마지막 정리 날짜
    FCurrentMonthTable: string;      // 현재 월 테이블 이름
    FLastTableCheck: TDateTime;      // 마지막으로 테이블 체크한 시간
    // Ext
    FQuery: TZQuery;
    FServerName: string;
    FDatabaseName: string;
    FUserName: string;
    FPassword: string;
    FConnected: Boolean;
    FRetentionMonths: Integer;
    FJsonLogPath: string;

    // 비동기 처리 관련 필드
    FLogQueue: TThreadList;          // 로그 메시지 큐

    function BuildSourceIdentifier(const ATag: string): string;
    function LogLevelToStr(ALevel: TLogLevel): string;

    // 테이블 관리 메서드
    function GetMonthlyTableName(const ADate: TDateTime): string;
    function GetCurrentMonthTableName: string;
    procedure CreateMonthlyTable(const ATableName, AYearMonth: string);
    procedure CreateMetaTable;
    procedure UpdateMetaTable(const ATableName, AYearMonth: string; ACreationDate: TDateTime);
    procedure UpdateTableRowCount(const ATableName: string);

    // 큐 및 로그 처리
    procedure FlushQueue;
    procedure CleanupOldLogs;
    procedure EnsureCurrentMonthTable;

    // Ext
    procedure InitializeTables;
    function GetLogRecordId(const LogItem: TLogItem): Integer;
    procedure WriteExtendedData(LogId: Integer; const ExtData: TLogExtendedData);
    function FormatDateTime(const Value: TDateTime): string;

    // JSON 관련 기능
    procedure WriteJsonLog(const LogItem: TLogItem; const ExtData: TLogExtendedData = nil);
    function GetJsonLogFileName: string;
    procedure SyncJsonToDatabase;
  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;

  public
    constructor Create(const AServerName, ADatabaseName, AUserName, APassword: string); reintroduce;
    destructor Destroy; override;
    // Ext
    procedure WriteLog(const LogItem: TLogItem); override;
    procedure WriteLogWithExtData(const LogItem: TLogItem; const ExtData: TLogExtendedData);
    // 로그 조회 및 관리
    procedure ExecuteLogMaintenance;
    function QueryLogs(const SQL: string): TZQuery;

    procedure Init; override;
    procedure Shutdown; override;

    // 데이터베이스 연결 설정
    procedure SetConnection(const AServerName, ADatabaseName, AUserName, APassword: string);

    // 로그 조회 메서드
    function GetLogs(const StartDate, EndDate: TDateTime;
                     const LogLevel: string = '';
                     const SearchText: string = ''): TZQuery;

    // 날짜 범위에 해당하는 테이블 목록 가져오기
    function GetTablesBetweenDates(StartDate, EndDate: TDateTime): TStringList;

    // 속성
    property TablePrefix: string read FTablePrefix write FTablePrefix;
    property MetaTableName: string read FMetaTableName write FMetaTableName;
    property AutoCreateTable: Boolean read FAutoCreateTable write FAutoCreateTable;
    property QueueMaxSize: Integer read FQueueMaxSize write FQueueMaxSize;
    property QueueFlushInterval: Integer read FQueueFlushInterval write FQueueFlushInterval;
    // Ext
    property RetentionMonths: Integer read FRetentionMonths write FRetentionMonths;
    property JsonLogPath: string read FJsonLogPath write FJsonLogPath;
    property Connected: Boolean read FConnected;
  end;

// GlobalLogger에서 DatabaseHandler 인스턴스를 찾는 함수
//function GetDatabaseHandler: TDatabaseHandler;

implementation

uses
  GlobalLogger, StrUtils;


{ TLogExtendedData }
constructor TLogExtendedData.Create;
begin
  inherited;
  FJsonObject := TMcJsonObject.Create;
end;

destructor TLogExtendedData.Destroy;
begin
  FJsonObject.Free;
  inherited;
end;

procedure TLogExtendedData.AddProperty(const Name: string; const Value: Variant);
begin
  case VarType(Value) of
    varInteger, varInt64, varByte, varSmallint, varShortInt, varWord, varLongWord:
      FJsonObject.Add(Name, Integer(Value));
    varSingle, varDouble, varCurrency:
      FJsonObject.Add(Name, Double(Value));
    varBoolean:
      FJsonObject.Add(Name, Boolean(Value));
    varNull:
      FJsonObject.AddNull(Name);
    else
      FJsonObject.Add(Name, String(Value));
  end;
end;

procedure TLogExtendedData.AddJsonObject(const Name: string; JsonObj: TMcJsonObject);
begin
  FJsonObject.Add(Name, JsonObj);
end;

procedure TLogExtendedData.AddJsonArray(const Name: string; JsonArr: TMcJsonArray);
begin
  FJsonObject.Add(Name, JsonArr);
end;

function TLogExtendedData.AsJson: string;
begin
  Result := FJsonObject.AsJson;
end;

function TLogExtendedData.AsJsonObject: TMcJsonObject;
begin
  Result := FJsonObject;
end;





{ TDatabaseHandler }
constructor TDatabaseHandler.Create(const AServerName, ADatabaseName, AUserName, APassword: string);
begin
  inherited Create;

  try
    // 기본값 설정
    FTablePrefix := 'LOGS';
    FMetaTableName := 'LOG_META';
    FAutoCreateTable := True;
    FLastCleanupDate := 0;           // 초기값 0으로 설정해 첫 로그 작성시 정리 실행
    FLastTableCheck := 0;            // 초기화
    FCurrentMonthTable := '';        // 아직 결정되지 않음

    // 데이터베이스 객체 생성
    FConnection := TZConnection.Create(nil);
    FQuery := TZQuery.Create(nil);
    FQuery.Connection := FConnection;
    FLogQuery := TZQuery.Create(nil);
    FLogQuery.Connection := FConnection;

    FServerName := AServerName;
    FDatabaseName := ADatabaseName;
    FUserName := AUserName;
    FPassword := APassword;
    FRetentionMonths := DEFAULT_RETENTION_MONTHS;

    // 기본 JSON 로그 경로 설정
    FJsonLogPath := IncludeTrailingPathDelimiter(ExtractFilePath(ParamStr(0)) + 'Logs\Json');
    if not DirectoryExists(FJsonLogPath) then
      ForceDirectories(FJsonLogPath);

    // 비동기 처리 관련
    FLogQueue := TThreadList.Create;
    FQueueMaxSize := 100;           // 기본 큐 크기
    FQueueFlushInterval := 5000;    // 기본 플러시 간격 (ms)
    FLastQueueFlush := Now;

    // 연결 정보 설정
    SetConnection(AServerName, ADatabaseName, AUserName, APassword);
  except
    on E: Exception do
      DebugToFile('DatabaseHandler 초기화 오류: ' + E.Message);
  end;
end;

destructor TDatabaseHandler.Destroy;
begin
  try
    Shutdown;

    // 로그 큐 정리
    FlushQueue;
    FLogQueue.Free;

    // 데이터베이스 객체 해제
    if Assigned(FQuery) then
      FreeAndNil(FQuery);
    if Assigned(FLogQuery) then
      FreeAndNil(FLogQuery);
    if Assigned(FConnection) then
      FreeAndNil(FConnection);
  except
    on E: Exception do
      DebugToFile('DatabaseHandler 소멸자 오류: ' + E.Message);
  end;

  inherited;
end;

procedure TDatabaseHandler.Init;
begin
  inherited;

  try
    // 데이터베이스 연결이 이미 설정되어 있으면 연결 시도
    if (FConnection.HostName <> '') and (FConnection.Database <> '') then
    begin
      if not FConnection.Connected then
      begin
        try
          FConnection.Connect;
        except
          on E: Exception do
          begin
            DebugToFile('DatabaseHandler 데이터베이스 연결 오류: ' + E.Message);
            Exit; // 연결 실패 시 더 이상 진행하지 않음
          end;
        end;

        // 테이블 자동 생성이 활성화되어 있으면 메타 테이블 생성
        if FAutoCreateTable then
        begin
          CreateMetaTable;

          // 현재 월에 해당하는 테이블 이름 초기화 및 생성 확인
          FCurrentMonthTable := GetCurrentMonthTableName;
          EnsureCurrentMonthTable;
        end;

        // 오래된 로그 정리 실행
        CleanupOldLogs;
      end;
    end;
  except
    on E: Exception do
      DebugToFile('DatabaseHandler 초기화 오류: ' + E.Message);
  end;
end;

procedure TDatabaseHandler.Shutdown;
begin
  try
    // 로그 큐 플러시
    FlushQueue;

    // 데이터베이스 연결 종료
    if FConnection.Connected then
      FConnection.Disconnect;
  except
    on E: Exception do
      DebugToFile('DatabaseHandler 종료 오류: ' + E.Message);
  end;

  inherited;
end;

procedure TDatabaseHandler.SetConnection(const AServerName, ADatabaseName, AUserName, APassword: string);
var
  AppPath: string;
  DbPath: string;
  LogDbFile: string;
begin
  try
    // 이미 연결되어 있으면 먼저 연결 종료
    if FConnection.Connected then
      FConnection.Disconnect;

    // 애플리케이션 경로와 DB 경로 설정
    AppPath := ExtractFilePath(ParamStr(0));
    DbPath := IncludeTrailingPathDelimiter(AppPath) + 'logs';

    // 로그 디렉토리가 없으면 생성
    if not DirectoryExists(DbPath) then
    begin
      try
        ForceDirectories(DbPath);
        DebugToFile('로그 디렉토리 생성: ' + DbPath);
      except
        on E: Exception do
        begin
          DebugToFile('로그 디렉토리 생성 실패: ' + E.Message);
          // 현재 디렉토리로 대체
          DbPath := AppPath;
        end;
      end;
    end;

    // DB 파일 경로 설정 (사용자 지정 이름이 있으면 사용, 없으면 기본 'Log.fdb' 사용)
    if ADatabase <> '' then
      LogDbFile := ADatabaseName
    else
      LogDbFile := 'Log.fdb';

    LogDbFile := IncludeTrailingPathDelimiter(DbPath) + LogDbFile;

    DebugToFile('데이터베이스 경로: ' + LogDbFile);

    // 연결 정보 설정
    FConnection.Protocol := 'firebird';
    FConnection.ClientCodepage := 'UTF8';
    FConnection.LibraryLocation := AppPath + 'fbclient.dll';
    FConnection.HostName := AServerName;
    FConnection.Database := LogDbFile;
    FConnection.User := AUserName;
    FConnection.Password := APassword;

    // DB가 없으면 생성
    if not FileExists(LogDbFile) then
    begin
      DebugToFile('데이터베이스 파일이 없음, 생성 시도: ' + LogDbFile);
      FConnection.Properties.Clear;
      FConnection.Properties.Values['dialect'] := '3';
      FConnection.Properties.Values['CreateNewDatabase'] :=
        'CREATE DATABASE ' + QuotedStr(LogDbFile) +
        ' USER ' + QuotedStr(AUserName) +
        ' PASSWORD ' + QuotedStr(APassword) +
        ' PAGE_SIZE 16384 DEFAULT CHARACTER SET UTF8';

      try
        FConnection.Connect;
        FConnected := FConnection.Connected;
        DebugToFile('데이터베이스 생성 성공');
      except
        on E: Exception do
          DebugToFile('데이터베이스 생성 실패: ' + E.Message);
      end;
    end
    else
    begin
      DebugToFile('기존 데이터베이스 파일에 연결 시도');
      try
        FConnection.Connect;
        FConnected := FConnection.Connected;
        DebugToFile('데이터베이스 연결 성공');
      except
        on E: Exception do
          DebugToFile('데이터베이스 연결 실패: ' + E.Message);
      end;
    end;

    // 테이블 생성 시도
    if FConnected and FAutoCreateTable then
    begin
      InitializeTables;

      CreateMetaTable;

      // 현재 월 테이블 이름 초기화 및 생성 확인
      FCurrentMonthTable := GetCurrentMonthTableName;
      EnsureCurrentMonthTable;
    end;

  except
    on E: Exception do
      DebugToFile('DatabaseHandler 연결 설정 오류: ' + E.Message);
  end;
end;

procedure TDatabaseHandler.InitializeTables;
begin
  // LOG_MASTER 테이블 생성
  FQuery.SQL.Text :=
    'EXECUTE BLOCK AS ' +
    'BEGIN ' +
    '  IF (NOT EXISTS(SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = ''LOG_MASTER'')) THEN ' +
    '  EXECUTE STATEMENT ' +
    '  ''CREATE TABLE LOG_MASTER ( ' +
    '    LOG_ID INTEGER NOT NULL PRIMARY KEY, ' +
    '    TIMESTAMP TIMESTAMP NOT NULL, ' +
    '    LEVEL VARCHAR(10) NOT NULL, ' +
    '    SOURCE VARCHAR(100), ' +
    '    MESSAGE VARCHAR(1000), ' +
    '    RAW_JSON BLOB SUB_TYPE TEXT, ' +  // JSON 저장용 컬럼 추가
    '    EXTRAS JSON'' ' +                 // Firebird 5.0의 JSON 기본 지원 활용
    '  END';
  FQuery.ExecSQL;

  // LOG_EXTENDED 테이블 생성
  FQuery.SQL.Text :=
    'EXECUTE BLOCK AS ' +
    'BEGIN ' +
    '  IF (NOT EXISTS(SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = ''LOG_EXTENDED'')) THEN ' +
    '  EXECUTE STATEMENT ' +
    '  ''CREATE TABLE LOG_EXTENDED ( ' +
    '    EXT_ID INTEGER NOT NULL PRIMARY KEY, ' +
    '    LOG_ID INTEGER NOT NULL, ' +
    '    EXT_TYPE VARCHAR(50) NOT NULL, ' +
    '    EXT_KEY VARCHAR(100) NOT NULL, ' +
    '    EXT_VALUE_STR VARCHAR(1000), ' +
    '    EXT_VALUE_NUM NUMERIC(18,4), ' +
    '    EXT_VALUE_BOOL BOOLEAN, ' +
    '    EXT_VALUE_DATE TIMESTAMP, ' +
    '    CONSTRAINT FK_LOG_EXT FOREIGN KEY (LOG_ID) REFERENCES LOG_MASTER(LOG_ID) ON DELETE CASCADE'' ' +
    '  END';
  FQuery.ExecSQL;

  // LOG_MASTER의 시퀀스 생성
  FQuery.SQL.Text :=
    'EXECUTE BLOCK AS ' +
    'BEGIN ' +
    '  IF (NOT EXISTS(SELECT 1 FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = ''GEN_LOG_ID'')) THEN ' +
    '  EXECUTE STATEMENT ''CREATE SEQUENCE GEN_LOG_ID''; ' +
    'END';
  FQuery.ExecSQL;

  // LOG_EXTENDED의 시퀀스 생성
  FQuery.SQL.Text :=
    'EXECUTE BLOCK AS ' +
    'BEGIN ' +
    '  IF (NOT EXISTS(SELECT 1 FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = ''GEN_LOG_EXT_ID'')) THEN ' +
    '  EXECUTE STATEMENT ''CREATE SEQUENCE GEN_LOG_EXT_ID''; ' +
    'END';
  FQuery.ExecSQL;

  // 인덱스 생성
  FQuery.SQL.Text :=
    'EXECUTE BLOCK AS ' +
    'BEGIN ' +
    '  IF (NOT EXISTS(SELECT 1 FROM RDB$INDICES WHERE RDB$INDEX_NAME = ''IDX_LOG_TIMESTAMP'')) THEN ' +
    '  EXECUTE STATEMENT ''CREATE INDEX IDX_LOG_TIMESTAMP ON LOG_MASTER(TIMESTAMP)''; ' +
    'END';
  FQuery.ExecSQL;

  FQuery.SQL.Text :=
    'EXECUTE BLOCK AS ' +
    'BEGIN ' +
    '  IF (NOT EXISTS(SELECT 1 FROM RDB$INDICES WHERE RDB$INDEX_NAME = ''IDX_LOG_LEVEL'')) THEN ' +
    '  EXECUTE STATEMENT ''CREATE INDEX IDX_LOG_LEVEL ON LOG_MASTER(LEVEL)''; ' +
    'END';
  FQuery.ExecSQL;
end;

function TDatabaseHandler.FormatDateTime(const Value: TDateTime): string;
begin
  Result := SysUtils.FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz', Value);
end;

procedure TDatabaseHandler.WriteLog(const LogItem: TLogItem);
begin
  WriteLogWithExtData(LogItem, nil);
end;

procedure TDatabaseHandler.WriteLogWithExtData(const LogItem: TLogItem; const ExtData: TLogExtendedData);
var
  LogId: Integer;
begin
  try
    // 항상 JSON 파일에 기록
    WriteJsonLog(LogItem, ExtData);

    if not FConnected then
      Exit;

    // 데이터베이스에 직접 기록
    FConnection.StartTransaction;
    try
      LogId := GetLogRecordId(LogItem);

      if Assigned(ExtData) then
        WriteExtendedData(LogId, ExtData);

      FConnection.Commit;
    except
      on E: Exception do
      begin
        FConnection.Rollback;
        raise;
      end;
    end;
  except
    // 에러 발생 시 핸들링 (파일에 기록 등)
    on E: Exception do
    begin
      // 여기서는 예외를 삼킴 (로거에서 예외가 발생하면 앱이 중단될 수 있음)
    end;
  end;
end;

function TDatabaseHandler.GetLogRecordId(const LogItem: TLogItem): Integer;
var
  JsonObj: TMcJsonObject;
  RawJson: string;
begin
  FQuery.SQL.Text := 'SELECT NEXT VALUE FOR GEN_LOG_ID FROM RDB$DATABASE';
  FQuery.Open;
  Result := FQuery.Fields[0].AsInteger;
  FQuery.Close;

  // JSON 객체 생성
  JsonObj := TMcJsonObject.Create;
  try
    JsonObj.Add('timestamp', FormatDateTime(LogItem.TimeStamp));
    JsonObj.Add('level', LogItem.Level.ToString);
    JsonObj.Add('source', LogItem.Source);
    JsonObj.Add('message', LogItem.Message);

    RawJson := JsonObj.AsJson;

    // 로그 레코드 삽입
    FQuery.SQL.Text :=
      'INSERT INTO LOG_MASTER (LOG_ID, TIMESTAMP, LEVEL, SOURCE, MESSAGE, RAW_JSON) ' +
      'VALUES (:LOG_ID, :TIMESTAMP, :LEVEL, :SOURCE, :MESSAGE, :RAW_JSON)';
    FQuery.ParamByName('LOG_ID').AsInteger := Result;
    FQuery.ParamByName('TIMESTAMP').AsDateTime := LogItem.TimeStamp;
    FQuery.ParamByName('LEVEL').AsString := LogItem.Level.ToString;
    FQuery.ParamByName('SOURCE').AsString := LogItem.Source;
    FQuery.ParamByName('MESSAGE').AsString := LogItem.Message;
    FQuery.ParamByName('RAW_JSON').AsString := RawJson;
    FQuery.ExecSQL;
  finally
    JsonObj.Free;
  end;
end;

procedure TDatabaseHandler.WriteExtendedData(LogId: Integer; const ExtData: TLogExtendedData);
begin
  // JSON 확장 데이터를 LOG_MASTER.EXTRAS에 저장
  FQuery.SQL.Text := 'UPDATE LOG_MASTER SET EXTRAS = :JSON_DATA WHERE LOG_ID = :LOG_ID';
  FQuery.ParamByName('JSON_DATA').AsString := ExtData.AsJson;
  FQuery.ParamByName('LOG_ID').AsInteger := LogId;
  FQuery.ExecSQL;
end;

function TDatabaseHandler.GetJsonLogFileName: string;
var
  DateDir: string;
begin
  // 날짜별 디렉토리 생성
  DateDir := IncludeTrailingPathDelimiter(FJsonLogPath + FormatDateTime('yyyy-mm-dd', Now));

  if not DirectoryExists(DateDir) then
    ForceDirectories(DateDir);

  Result := DateDir + FormatDateTime('yyyy-mm-dd', Now) + '.jsonl';
end;

procedure TDatabaseHandler.WriteJsonLog(const LogItem: TLogItem; const ExtData: TLogExtendedData = nil);
var
  LogFile: TextFile;
  JsonObj: TMcJsonObject;
  FilePath: string;
begin
  FilePath := GetJsonLogFileName;

  // JSON 객체 생성
  JsonObj := TMcJsonObject.Create;
  try
    // 기본 로그 정보 추가
    JsonObj.Add('timestamp', FormatDateTime(LogItem.TimeStamp));
    JsonObj.Add('level', LogItem.Level.ToString);
    JsonObj.Add('source', LogItem.Source);
    JsonObj.Add('message', LogItem.Message);

    // 확장 데이터가 있으면 추가
    if Assigned(ExtData) then
      JsonObj.Add('metadata', ExtData.AsJsonObject);

    // JSON Lines 형식으로 파일에 쓰기
    AssignFile(LogFile, FilePath);
    if FileExists(FilePath) then
      Append(LogFile)
    else
      Rewrite(LogFile);

    try
      WriteLn(LogFile, JsonObj.AsJson);
    finally
      CloseFile(LogFile);
    end;

  finally
    JsonObj.Free;
  end;
end;

procedure TDatabaseHandler.SyncJsonToDatabase;
var
  SearchRec: TSearchRec;
  FilePath, Line: string;
  LogFile: TextFile;
  JsonObj: TMcJsonObject;
  LogItem: TLogItem;
  ExtData: TLogExtendedData;
begin
  if not FConnected then
    Exit;

  if FindFirst(IncludeTrailingPathDelimiter(FJsonLogPath) + '*.jsonl', faAnyFile, SearchRec) = 0 then
  begin
    try
      repeat
        FilePath := IncludeTrailingPathDelimiter(FJsonLogPath) + SearchRec.Name;

        // 파일의 각 라인에서 JSON 파싱
        AssignFile(LogFile, FilePath);
        try
          Reset(LogFile);

          FConnection.StartTransaction;
          try
            while not Eof(LogFile) do
            begin
              ReadLn(LogFile, Line);

              if Trim(Line) <> '' then
              begin
                JsonObj := TMcJsonObject.Create;
                try
                  JsonObj.Parse(Line);

                  // LogItem 생성
                  LogItem := TLogItem.Create;
                  try
                    LogItem.TimeStamp := ISO8601ToDate(JsonObj.S['timestamp'], True);
                    LogItem.Level := TLogLevel(GetEnumValue(TypeInfo(TLogLevel), 'll' + JsonObj.S['level']));
                    LogItem.Source := JsonObj.S['source'];
                    LogItem.Message := JsonObj.S['message'];

                    // 확장 데이터 처리
                    if JsonObj.Contains('metadata') then
                    begin
                      ExtData := TLogExtendedData.Create;
                      try
                        ExtData.AddJsonObject('metadata', JsonObj.O['metadata']);
                        WriteLogWithExtData(LogItem, ExtData);
                      finally
                        ExtData.Free;
                      end;
                    end
                    else
                      WriteLog(LogItem);

                  finally
                    LogItem.Free;
                  end;
                finally
                  JsonObj.Free;
                end;
              end;
            end;

            FConnection.Commit;
          except
            FConnection.Rollback;
            raise;
          end;

        finally
          CloseFile(LogFile);
        end;

        // 처리된 파일 이동 또는 삭제 (옵션)
        // RenameFile(FilePath, FilePath + '.processed');

      until FindNext(SearchRec) <> 0;
    finally
      FindClose(SearchRec);
    end;
  end;
end;

procedure TDatabaseHandler.ExecuteLogMaintenance;
var
  CutoffDate: TDateTime;
begin
  if not FConnected then
    Exit;

  CutoffDate := IncMonth(Now, -FRetentionMonths);

  FQuery.SQL.Text :=
    'DELETE FROM LOG_MASTER WHERE TIMESTAMP < :CUTOFF_DATE';
  FQuery.ParamByName('CUTOFF_DATE').AsDateTime := CutoffDate;
  FQuery.ExecSQL;
end;

function TDatabaseHandler.QueryLogs(const SQL: string): TZQuery;
var
  Query: TZQuery;
begin
  Query := TZQuery.Create(nil);
  Query.Connection := FConnection;
  Query.SQL.Text := SQL;
  Query.Open;
  Result := Query;
end;






// 월별 테이블 이름 생성 (YYYYMM 형식)
function TDatabaseHandler.GetMonthlyTableName(const ADate: TDateTime): string;
begin
  Result := Format('%s_%s', [FTablePrefix, FormatDateTime('YYYYMM', ADate)]);
end;

// 현재 월에 해당하는 테이블 이름 가져오기
function TDatabaseHandler.GetCurrentMonthTableName: string;
begin
  Result := GetMonthlyTableName(Date);
end;


// 메타 테이블 생성
procedure TDatabaseHandler.CreateMetaTable;
begin
  try
    if not FConnection.Connected then
    begin
      try
        FConnection.Connect;
      except
        on E: Exception do
        begin
          DebugToFile('DatabaseHandler 메타 테이블 생성 중 연결 오류: ' + E.Message);
          Exit;
        end;
      end;
    end;

    try
      // 메타 테이블 존재 여부 확인
      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = ''' + UpperCase(FMetaTableName) + '''';
      FLogQuery.Open;

      if FLogQuery.Fields[0].AsInteger = 0 then
      begin
        // 테이블이 없으면 생성 - ROW_COUNT 대신 RECORD_COUNT 사용
        FLogQuery.Close;
        FLogQuery.SQL.Text :=
          'CREATE TABLE ' + FMetaTableName + ' (' +
          '  TABLE_NAME VARCHAR(63) NOT NULL PRIMARY KEY,' +
          '  YEAR_MONTH VARCHAR(6) NOT NULL,' +
          '  CREATION_DATE DATE NOT NULL,' +
          '  LAST_UPDATE DATE NOT NULL,' +
          '  RECORD_COUNT INTEGER DEFAULT 0' +
          ')';

        DebugToFile('메타 테이블 생성 시도: ' + FLogQuery.SQL.Text);
        FLogQuery.ExecSQL;
        DebugToFile('메타 테이블 생성 성공');

        // 인덱스 생성
        FLogQuery.SQL.Text :=
          'CREATE INDEX IDX_' + FMetaTableName + '_YM ON ' + FMetaTableName + ' (YEAR_MONTH)';
        FLogQuery.ExecSQL;
        DebugToFile('메타 테이블 인덱스 생성 성공');
      end
      else
      begin
        FLogQuery.Close;
        DebugToFile('메타 테이블이 이미 존재함: ' + FMetaTableName);
      end;
    except
      on E: Exception do
        DebugToFile('DatabaseHandler 메타 테이블 생성 SQL 오류: ' + E.Message);
    end;
  except
    on E: Exception do
      DebugToFile('DatabaseHandler 메타 테이블 생성 오류: ' + E.Message);
  end;
end;

// 월별 로그 테이블 생성
procedure TDatabaseHandler.CreateMonthlyTable(const ATableName, AYearMonth: string);
begin
  try
    if not FConnection.Connected then
    begin
      try
        FConnection.Connect;
      except
        on E: Exception do
        begin
          DebugToFile('DatabaseHandler 테이블 생성 중 연결 오류: ' + E.Message);
          Exit;
        end;
      end;
    end;

    try
      // 테이블 존재 여부 확인
      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = ''' + UpperCase(ATableName) + '''';
      FLogQuery.Open;

      if FLogQuery.Fields[0].AsInteger = 0 then
      begin
        // 테이블이 없으면 생성 - Firebird 5.0의 TIME WITHOUT TIME ZONE 사용
        FLogQuery.Close;
        FLogQuery.SQL.Text :=
          'CREATE TABLE ' + ATableName + ' (' +
          '  ID INTEGER NOT NULL PRIMARY KEY,' +
          '  LDATE DATE NOT NULL,' +
          '  LTIME TIME WITHOUT TIME ZONE NOT NULL,' + // 밀리초까지 저장 가능한 시간 타입
          '  LLEVEL VARCHAR(20) NOT NULL,' +
          '  LSOURCE VARCHAR(100),' +
          '  LMESSAGE VARCHAR(4000)' +
          ')';

        DebugToFile('테이블 생성 시도: ' + FLogQuery.SQL.Text);
        FLogQuery.ExecSQL;
        DebugToFile('테이블 생성 성공: ' + ATableName);

        // 시퀀스 생성
        FLogQuery.SQL.Text := 'CREATE SEQUENCE SEQ_' + ATableName;
        FLogQuery.ExecSQL;
        DebugToFile('시퀀스 생성 성공: SEQ_' + ATableName);

        // 트리거 생성
        FLogQuery.SQL.Text :=
          'CREATE TRIGGER ' + ATableName + '_BI FOR ' + ATableName + ' ' +
          'ACTIVE BEFORE INSERT POSITION 0 AS ' +
          'BEGIN ' +
          '  IF (NEW.ID IS NULL) THEN ' +
          '    NEW.ID = NEXT VALUE FOR SEQ_' + ATableName + '; ' +
          'END';
        FLogQuery.ExecSQL;
        DebugToFile('트리거 생성 성공: ' + ATableName + '_BI');

        // 인덱스 생성 - 날짜, 레벨, 소스에 대한 인덱스
        FLogQuery.SQL.Text :=
          'CREATE INDEX IDX_' + ATableName + '_DATE ON ' + ATableName + ' (LDATE)';
        FLogQuery.ExecSQL;

        FLogQuery.SQL.Text :=
          'CREATE INDEX IDX_' + ATableName + '_LEVEL ON ' + ATableName + ' (LLEVEL)';
        FLogQuery.ExecSQL;

        FLogQuery.SQL.Text :=
          'CREATE INDEX IDX_' + ATableName + '_SOURCE ON ' + ATableName + ' (LSOURCE)';
        FLogQuery.ExecSQL;

        DebugToFile('인덱스 생성 성공: ' + ATableName);

        // 메타 테이블 업데이트
        UpdateMetaTable(ATableName, AYearMonth, Now);
      end
      else
      begin
        FLogQuery.Close;
        DebugToFile('테이블이 이미 존재함: ' + ATableName);

        // 메타 테이블에 없으면 추가
        UpdateMetaTable(ATableName, AYearMonth, Now);
      end;
    except
      on E: Exception do
        DebugToFile('DatabaseHandler 테이블 생성 SQL 오류: ' + E.Message);
    end;
  except
    on E: Exception do
      DebugToFile('DatabaseHandler 테이블 생성 오류: ' + E.Message);
  end;
end;

// 메타 테이블 업데이트
procedure TDatabaseHandler.UpdateMetaTable(const ATableName, AYearMonth: string; ACreationDate: TDateTime);
begin
  try
    // 메타 테이블에 해당 테이블 정보가 있는지 확인
    FLogQuery.Close;
    FLogQuery.SQL.Text :=
      'SELECT COUNT(*) FROM ' + FMetaTableName + ' WHERE TABLE_NAME = :TableName';
    FLogQuery.ParamByName('TableName').AsString := ATableName;
    FLogQuery.Open;

    if FLogQuery.Fields[0].AsInteger = 0 then
    begin
      // 없으면 새로 추가
      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'INSERT INTO ' + FMetaTableName + ' (TABLE_NAME, YEAR_MONTH, CREATION_DATE, LAST_UPDATE, RECORD_COUNT) ' +
        'VALUES (:TableName, :YearMonth, :CreationDate, :LastUpdate, 0)';
      FLogQuery.ParamByName('TableName').AsString := ATableName;
      FLogQuery.ParamByName('YearMonth').AsString := AYearMonth;
      FLogQuery.ParamByName('CreationDate').AsDate := ACreationDate;
      FLogQuery.ParamByName('LastUpdate').AsDate := ACreationDate;
      FLogQuery.ExecSQL;

      DebugToFile('메타 테이블에 새 테이블 정보 추가: ' + ATableName);
    end
    else
    begin
      // 있으면 마지막 업데이트 시간만 갱신
      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'UPDATE ' + FMetaTableName + ' SET LAST_UPDATE = :LastUpdate ' +
        'WHERE TABLE_NAME = :TableName';
      FLogQuery.ParamByName('LastUpdate').AsDate := Now;
      FLogQuery.ParamByName('TableName').AsString := ATableName;
      FLogQuery.ExecSQL;

      DebugToFile('메타 테이블 정보 업데이트: ' + ATableName);
    end;
  except
    on E: Exception do
      DebugToFile('메타 테이블 업데이트 오류: ' + E.Message);
  end;
end;

// 테이블의 행 수 업데이트
procedure TDatabaseHandler.UpdateTableRowCount(const ATableName: string);
var
  RowCount: Integer;
begin
  try
    // 테이블 행 수 조회
    FLogQuery.Close;
    FLogQuery.SQL.Text := 'SELECT COUNT(*) FROM ' + ATableName;
    FLogQuery.Open;
    RowCount := FLogQuery.Fields[0].AsInteger;
    FLogQuery.Close;

    // 메타 테이블 업데이트
    FLogQuery.SQL.Text :=
      'UPDATE ' + FMetaTableName + ' SET RECORD_COUNT = :RowCount, LAST_UPDATE = :LastUpdate ' +
      'WHERE TABLE_NAME = :TableName';
    FLogQuery.ParamByName('RowCount').AsInteger := RowCount;
    FLogQuery.ParamByName('LastUpdate').AsDate := Now;
    FLogQuery.ParamByName('TableName').AsString := ATableName;
    FLogQuery.ExecSQL;
  except
    on E: Exception do
      DebugToFile('테이블 행 수 업데이트 오류: ' + E.Message);
  end;
end;

// 현재 월 테이블이 있는지 확인하고 없으면 생성
procedure TDatabaseHandler.EnsureCurrentMonthTable;
var
  CurrentMonth: string;
  CurrentTable: string;
begin
  // 마지막 체크 이후 하루가 지났으면 다시 체크
  if MinutesBetween(Now, FLastTableCheck) < 60 then
    Exit;

  try
    // 현재 월에 해당하는 테이블 이름과 월 문자열 가져오기
    CurrentTable := GetCurrentMonthTableName;
    CurrentMonth := FormatDateTime('YYYYMM', Date);

    // 현재 설정된 테이블이 이번 달 테이블이 아니면 새로 설정
    if FCurrentMonthTable <> CurrentTable then
    begin
      FCurrentMonthTable := CurrentTable;
      DebugToFile('현재 월 테이블 변경: ' + FCurrentMonthTable);
    end;

    // 테이블이 없으면 생성
    CreateMonthlyTable(FCurrentMonthTable, CurrentMonth);

    // 마지막 체크 시간 업데이트
    FLastTableCheck := Now;
  except
    on E: Exception do
      DebugToFile('현재 월 테이블 확인 오류: ' + E.Message);
  end;
end;

// 날짜 범위에 해당하는 테이블 목록 가져오기
function TDatabaseHandler.GetTablesBetweenDates(StartDate, EndDate: TDateTime): TStringList;
var
  StartYearMonth, EndYearMonth: string;
  CurrentDate: TDateTime;
  TableName: string;
  YearMonth: string;
begin
  Result := TStringList.Create;

  try
    // 날짜 범위를 YYYYMM 형식의 문자열로 변환
    StartYearMonth := FormatDateTime('YYYYMM', StartOfTheMonth(StartDate));
    EndYearMonth := FormatDateTime('YYYYMM', StartOfTheMonth(EndDate));

    // 메타 테이블이 있는지 확인
    FLogQuery.Close;
    FLogQuery.SQL.Text :=
      'SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = ''' + UpperCase(FMetaTableName) + '''';
    FLogQuery.Open;

    if FLogQuery.Fields[0].AsInteger > 0 then
    begin
      // 메타 테이블에서 조회
      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'SELECT TABLE_NAME FROM ' + FMetaTableName + ' ' +
        'WHERE YEAR_MONTH >= :StartYM AND YEAR_MONTH <= :EndYM ' +
        'ORDER BY YEAR_MONTH';
      FLogQuery.ParamByName('StartYM').AsString := StartYearMonth;
      FLogQuery.ParamByName('EndYM').AsString := EndYearMonth;
      FLogQuery.Open;

      // 결과를 리스트에 추가
      while not FLogQuery.EOF do
      begin
        Result.Add(FLogQuery.FieldByName('TABLE_NAME').AsString);
        FLogQuery.Next;
      end;
    end
    else
    begin
      // 메타 테이블이 없으면 테이블 이름 패턴으로 조회
      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'SELECT RDB$RELATION_NAME FROM RDB$RELATIONS ' +
        'WHERE RDB$RELATION_NAME LIKE ''' + UpperCase(FTablePrefix) + '_%'' ' +
        'AND RDB$SYSTEM_FLAG = 0 ' +
        'ORDER BY RDB$RELATION_NAME';
      FLogQuery.Open;

      // 결과에서 날짜 범위에 해당하는 테이블만 필터링
      while not FLogQuery.EOF do
      begin
        TableName := Trim(FLogQuery.FieldByName('RDB$RELATION_NAME').AsString);

        // 테이블 이름에서 YYYYMM 부분 추출
        if Length(TableName) >= Length(FTablePrefix) + 7 then
        begin
          YearMonth := Copy(TableName, Length(FTablePrefix) + 2, 6);

          // 날짜 범위에 포함되는지 확인
          if (YearMonth >= StartYearMonth) and (YearMonth <= EndYearMonth) then
            Result.Add(TableName);
        end;

        FLogQuery.Next;
      end;
    end;

    // 결과가 없으면 현재 테이블만 포함
    if Result.Count = 0 then
    begin
      // 현재 달 테이블이라도 확인
      EnsureCurrentMonthTable;
      Result.Add(FCurrentMonthTable);
    end;
  except
    on E: Exception do
    begin
      DebugToFile('날짜 범위 테이블 조회 오류: ' + E.Message);
      Result.Clear; // 오류 시 빈 리스트 반환
    end;
  end;
end;

// 로그 조회 메서드
function TDatabaseHandler.GetLogs(const StartDate, EndDate: TDateTime;
                                  const LogLevel: string = '';
                                  const SearchText: string = ''): TZQuery;
var
  SQL: TStringList;
  Tables: TStringList;
  i: Integer;
  LevelFilter, SearchFilter: string;
  Query: TZQuery;
begin
  Query := TZQuery.Create(nil);
  Query.Connection := FConnection;

  SQL := TStringList.Create;
  Tables := nil;

  try
    // 날짜 범위에 해당하는 테이블 목록 가져오기
    Tables := GetTablesBetweenDates(StartDate, EndDate);

    // 테이블이 없으면 빈 결과셋 반환
    if Tables.Count = 0 then
    begin
      SQL.Add('SELECT 0 AS ID, CAST(''1900-01-01'' AS DATE) AS LDATE,');
      SQL.Add('CAST(''00:00:00'' AS TIME) AS LTIME,');
      SQL.Add('''NONE'' AS LLEVEL, '''' AS LSOURCE, ''데이터 없음'' AS LMESSAGE');
      SQL.Add('FROM RDB$DATABASE WHERE 1=0');

      Query.SQL.Text := SQL.Text;
      Query.Open;
      Result := Query;
      Exit;
    end;

    // 필터 준비
    if LogLevel <> '' then
      LevelFilter := Format('LLEVEL = ''%s''', [LogLevel])
    else
      LevelFilter := '';

    if SearchText <> '' then
    begin
      SearchFilter := Format('(LSOURCE LIKE ''%%%s%%'' OR LMESSAGE LIKE ''%%%s%%'')',
                           [SearchText, SearchText]);
    end
    else
      SearchFilter := '';

    // 여러 테이블을 UNION ALL로 결합
    for i := 0 to Tables.Count - 1 do
    begin
      if i > 0 then
        SQL.Add('UNION ALL');

      SQL.Add(Format('SELECT ID, LDATE, LTIME, LLEVEL, LSOURCE, LMESSAGE FROM %s', [Tables[i]]));
      SQL.Add('WHERE LDATE >= :StartDate AND LDATE <= :EndDate');

      if LevelFilter <> '' then
        SQL.Add('AND ' + LevelFilter);

      if SearchFilter <> '' then
        SQL.Add('AND ' + SearchFilter);
    end;

    // 최종 정렬
    SQL.Add('ORDER BY LDATE DESC, LTIME DESC');

    // 쿼리 실행
    Query.SQL.Text := SQL.Text;
    Query.ParamByName('StartDate').AsDate := StartDate;
    Query.ParamByName('EndDate').AsDate := EndDate;
    Query.Open;

    Result := Query;
  finally
    SQL.Free;
    if Assigned(Tables) then
      Tables.Free;
  end;
end;

function TDatabaseHandler.LogLevelToStr(ALevel: TLogLevel): string;
begin
  case ALevel of
    //llTrace: Result := 'TRACE';
    llDebug: Result := 'DEBUG';
    llInfo: Result := 'INFO';
    llWarning: Result := 'WARNING';
    llError: Result := 'ERROR';
    llFatal: Result := 'FATAL';
    else Result := 'UNKNOWN';
  end;
end;

function TDatabaseHandler.BuildSourceIdentifier(const ATag: string): string;
var
  ProcessID: Cardinal;
  ThreadID: Cardinal;
begin
  ProcessID := GetProcessID;
  ThreadID := GetCurrentThreadID;

  // 소스 식별자 형식: TAG-ProcessID-ThreadID
  if ATag <> '' then
    Result := Format('%s-%d-%d', [ATag, ProcessID, ThreadID])
  else
    Result := Format('APP-%d-%d', [ProcessID, ThreadID]);
end;

procedure TDatabaseHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  LogItem: PDBLogQueueItem;
  List: TList;
  SourceTag, LogMessage: string;
  LevelStr: string;
  CurrentTime: TDateTime;
  Start, End_: Integer;
begin
  // 현재 월 테이블 확인 및 필요시 생성
  EnsureCurrentMonthTable;

  // 하루에 한 번 오래된 로그 정리
  if DaysBetween(Now, FLastCleanupDate) >= 1 then
    CleanupOldLogs;

  // 로그 레벨을 문자열로 변환
  LevelStr := LogLevelToStr(Level);

  // 현재 시간 가져오기 (밀리초 포함)
  CurrentTime := Now;

  // 원본 메시지에서 태그 부분과 실제 메시지 부분 분리
  // 메시지 형식: [yyyy-mm-dd hh:nn:ss.zzz] [LEVEL] [Source] Message
  if Pos('[', Msg) = 1 then
  begin
    // 태그가 포함된 형식 ([시간][레벨][소스] 메시지)
    SourceTag := LevelStr;
    LogMessage := Msg;

    // 레벨 정보가 이미 메시지에 포함되어 있으면 그대로 사용
    if Pos('[' + LevelStr + ']', Msg) > 0 then
    begin
      // 메시지에서 소스 태그 추출 시도
      Start := Pos('[', Msg, Pos(']', Msg, Pos(']', Msg) + 1) + 1);
      if Start > 0 then
      begin
        End_ := Pos(']', Msg, Start);
        if End_ > 0 then
          SourceTag := Copy(Msg, Start + 1, End_ - Start - 1);
      end;

      // 실제 메시지 부분 추출
      Start := Pos(']', Msg, Pos(']', Msg, Pos(']', Msg) + 1) + 1);
      if Start > 0 then
        LogMessage := Trim(Copy(Msg, Start + 1, Length(Msg)));
    end;
  end
  else
  begin
    // 태그가 없는 단순 메시지
    SourceTag := LevelStr;
    LogMessage := Msg;
  end;

  if AsyncMode = amThread then
  begin
    // 비동기 모드: 큐에 메시지 추가
    New(LogItem);
    LogItem^.Message := LogMessage;  // 실제 메시지 부분만 저장
    LogItem^.Level := Level;
    LogItem^.Tag := SourceTag;       // 추출된 소스 태그 사용
    LogItem^.Timestamp := CurrentTime; // 타임스탬프 저장 (테이블 선택용)

    List := FLogQueue.LockList;
    try
      List.Add(LogItem);

      // 큐 크기 확인 및 필요시 플러시
      if List.Count >= FQueueMaxSize then
        FlushQueue;
    finally
      FLogQueue.UnlockList;
    end;

    // 자동 플러시 확인 - 마지막 플러시 이후 지정된 시간이 지났으면 큐 플러시
    if MilliSecondsBetween(Now, FLastQueueFlush) >= FQueueFlushInterval then
      FlushQueue;
  end
  else
  begin
    // 동기 모드: 직접 데이터베이스에 기록
    try
      if not FConnection.Connected then
        FConnection.Connect;

      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'INSERT INTO ' + FCurrentMonthTable + ' (LDATE, LTIME, LLEVEL, LSOURCE, LMESSAGE) ' +
        'VALUES (:LDATE, :LTIME, :LLEVEL, :LSOURCE, :LMESSAGE)';

      FLogQuery.ParamByName('LDATE').AsDate := Date;
      FLogQuery.ParamByName('LTIME').AsTime := CurrentTime; // 시간 타입으로 저장 (밀리초 포함)
      FLogQuery.ParamByName('LLEVEL').AsString := LevelStr;
      FLogQuery.ParamByName('LSOURCE').AsString := SourceTag;
      FLogQuery.ParamByName('LMESSAGE').AsString := LogMessage;

      FLogQuery.ExecSQL;

      // 주기적으로 메타 테이블 업데이트 (한 시간에 한 번)
      if HoursBetween(Now, FLastTableCheck) >= 1 then
      begin
        UpdateTableRowCount(FCurrentMonthTable);
        FLastTableCheck := Now;
      end;
    except
      on E: Exception do
        DebugToFile('DatabaseHandler 로그 작성 오류: ' + E.Message);
    end;
  end;
end;

procedure TDatabaseHandler.FlushQueue;
var
  List: TList;
  i, j: Integer;  // 별도의 루프 변수 사용
  LogItem: PDBLogQueueItem;
  TableItems: TStringList;
  CurrentDate: TDateTime;
  MonthTable: string;
  ItemsStr, ValuesStr: string;
  BatchSize: Integer;
begin
  List := FLogQueue.LockList;
  try
    if List.Count = 0 then
      Exit;

    try
      if not FConnection.Connected then
        FConnection.Connect;

      TableItems := TStringList.Create;
      try
        // 각 아이템을 해당 월 테이블로 분류
        for i := 0 to List.Count - 1 do
        begin
          LogItem := PDBLogQueueItem(List[i]);
          CurrentDate := LogItem^.Timestamp;
          MonthTable := GetMonthlyTableName(CurrentDate);

          if TableItems.IndexOf(MonthTable) < 0 then
          begin
            CreateMonthlyTable(MonthTable, FormatDateTime('YYYYMM', CurrentDate));
            TableItems.Add(MonthTable);
          end;
        end;

        if not FConnection.InTransaction then
          FConnection.StartTransaction;

        // 테이블별로 배치 삽입 수행
        for j := 0 to TableItems.Count - 1 do  // 외부 루프는 j 사용
        begin
          MonthTable := TableItems[j];
          BatchSize := 0;
          ItemsStr := '';
          ValuesStr := '';

          // 해당 테이블에 속한 아이템들 배치 처리
          for i := 0 to List.Count - 1 do  // 내부 루프는 i 사용
          begin
            LogItem := PDBLogQueueItem(List[i]);
            CurrentDate := LogItem^.Timestamp;

            if GetMonthlyTableName(CurrentDate) = MonthTable then
            begin
              if BatchSize > 0 then
                ValuesStr := ValuesStr + ', ';

              ValuesStr := ValuesStr + Format('(%s, %s, ''%s'', ''%s'', ''%s'')',
                           [QuotedStr(FormatDateTime('yyyy-mm-dd', DateOf(CurrentDate))),
                            QuotedStr(FormatDateTime('hh:nn:ss.zzz', TimeOf(CurrentDate))),
                            LogLevelToStr(LogItem^.Level),
                            StringReplace(LogItem^.Tag, '''', '''''', [rfReplaceAll]),
                            StringReplace(LogItem^.Message, '''', '''''', [rfReplaceAll])]);

              Inc(BatchSize);

              if BatchSize >= 50 then
              begin
                FLogQuery.Close;
                FLogQuery.SQL.Text := Format('INSERT INTO %s (LDATE, LTIME, LLEVEL, LSOURCE, LMESSAGE) VALUES %s',
                                       [MonthTable, ValuesStr]);
                FLogQuery.ExecSQL;

                BatchSize := 0;
                ValuesStr := '';
              end;
            end;
          end;

          if BatchSize > 0 then
          begin
            FLogQuery.Close;
            FLogQuery.SQL.Text := Format('INSERT INTO %s (LDATE, LTIME, LLEVEL, LSOURCE, LMESSAGE) VALUES %s',
                               [MonthTable, ValuesStr]);
            FLogQuery.ExecSQL;
          end;

          UpdateMetaTable(MonthTable, Copy(MonthTable, Length(FTablePrefix) + 2, 6), Now);
          UpdateTableRowCount(MonthTable);
        end;

        if FConnection.InTransaction then
          FConnection.Commit;

        for i := 0 to List.Count - 1 do
        begin
          LogItem := PDBLogQueueItem(List[i]);
          Dispose(LogItem);
        end;

        List.Clear;
      finally
        TableItems.Free;
      end;

      FLastQueueFlush := Now;
    except
      on E: Exception do
      begin
        if FConnection.InTransaction then
          FConnection.Rollback;

        DebugToFile('DatabaseHandler 큐 플러시 오류: ' + E.Message);
      end;
    end;
  finally
    FLogQueue.UnlockList;
  end;
end;

procedure TDatabaseHandler.CleanupOldLogs;
var
  CutoffDate: TDateTime;
  YearMonth: Integer;
  CurrentYearMonth: Integer;
  i: Integer;
  TableName: string;
  YearMonthStr: string;
begin
  try
    // 하루에 한 번만 정리 실행
    if DaysBetween(Now, FLastCleanupDate) < 1 then
      Exit;

    if not FConnection.Connected then
      FConnection.Connect;

    // 보관 기간이 0 이하인 경우 정리하지 않음
    if FRetentionMonths <= 0 then
      Exit;

    // 현재 날짜의 YYYYMM 값 계산
    CurrentYearMonth := StrToInt(FormatDateTime('YYYYMM', Date));

    // 보관 기간에 따른 기준 날짜 계산 (오늘로부터 X개월 전)
    CutoffDate := IncMonth(Date, -FRetentionMonths);

    // 메타 테이블이 있는지 확인
    FLogQuery.Close;
    FLogQuery.SQL.Text :=
      'SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = ''' + UpperCase(FMetaTableName) + '''';
    FLogQuery.Open;

    if FLogQuery.Fields[0].AsInteger > 0 then
    begin
      // 메타 테이블에서 오래된 테이블 목록 조회
      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'SELECT TABLE_NAME, YEAR_MONTH FROM ' + FMetaTableName +
        ' ORDER BY YEAR_MONTH';
      FLogQuery.Open;

      while not FLogQuery.EOF do
      begin
        YearMonth := StrToIntDef(FLogQuery.FieldByName('YEAR_MONTH').AsString, 0);
        TableName := FLogQuery.FieldByName('TABLE_NAME').AsString;

        // 보관 기간보다 오래된 테이블 삭제
        if (YearMonth > 0) and (YearMonth < StrToInt(FormatDateTime('YYYYMM', CutoffDate))) then
        begin
          try
            // 메타 테이블에서 삭제
            FLogQuery.Close;
            FLogQuery.SQL.Text := 'DELETE FROM ' + FMetaTableName + ' WHERE TABLE_NAME = :TableName';
            FLogQuery.ParamByName('TableName').AsString := TableName;
            FLogQuery.ExecSQL;

            // 시퀀스 삭제
            FLogQuery.Close;
            FLogQuery.SQL.Text := 'DROP SEQUENCE SEQ_' + TableName;
            FLogQuery.ExecSQL;

            // 테이블 삭제
            FLogQuery.Close;
            FLogQuery.SQL.Text := 'DROP TABLE ' + TableName;
            FLogQuery.ExecSQL;

            DebugToFile(Format('오래된 로그 테이블 삭제: %s (%s)',
                           [TableName, FormatDateTime('YYYYMM', CutoffDate)]));
          except
            on E: Exception do
              DebugToFile('테이블 삭제 오류: ' + TableName + ' - ' + E.Message);
          end;
        end;

        FLogQuery.Next;
      end;
    end
    else
    begin
      // 메타 테이블이 없으면 테이블 이름 패턴으로 조회
      FLogQuery.Close;
      FLogQuery.SQL.Text :=
        'SELECT RDB$RELATION_NAME FROM RDB$RELATIONS ' +
        'WHERE RDB$RELATION_NAME LIKE ''' + UpperCase(FTablePrefix) + '_%'' ' +
        'AND RDB$SYSTEM_FLAG = 0';
      FLogQuery.Open;

      while not FLogQuery.EOF do
      begin
        TableName := Trim(FLogQuery.FieldByName('RDB$RELATION_NAME').AsString);

        // 테이블 이름에서 YYYYMM 부분 추출
        if Length(TableName) >= Length(FTablePrefix) + 7 then
        begin
          YearMonthStr := Copy(TableName, Length(FTablePrefix) + 2, 6);
          YearMonth := StrToIntDef(YearMonthStr, 0);

          // 보관 기간보다 오래된 테이블 삭제
          if (YearMonth > 0) and (YearMonth < StrToInt(FormatDateTime('YYYYMM', CutoffDate))) then
          begin
            try
              // 시퀀스 삭제
              FLogQuery.Close;
              FLogQuery.SQL.Text := 'DROP SEQUENCE SEQ_' + TableName;
              FLogQuery.ExecSQL;

              // 테이블 삭제
              FLogQuery.Close;
              FLogQuery.SQL.Text := 'DROP TABLE ' + TableName;
              FLogQuery.ExecSQL;

              DebugToFile(Format('오래된 로그 테이블 삭제: %s (%s)',
                             [TableName, YearMonthStr]));
            except
              on E: Exception do
                DebugToFile('테이블 삭제 오류: ' + TableName + ' - ' + E.Message);
            end;
          end;
        end;

        FLogQuery.Next;
      end;
    end;

    // 마지막 정리 날짜 업데이트
    FLastCleanupDate := Now;

    DebugToFile(Format('DatabaseHandler 로그 정리 완료: %d개월 이전 로그 삭제',
                       [FRetentionMonths]));
  except
    on E: Exception do
      DebugToFile('DatabaseHandler 로그 정리 오류: ' + E.Message);
  end;
end;

end.
