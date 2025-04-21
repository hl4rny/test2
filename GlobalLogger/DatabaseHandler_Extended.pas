unit DatabaseHandler;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, ZConnection, ZDataset, ZDbcIntfs, DateUtils, Dialogs,
  LogHandlers, SourceIdentifier, LogServerHandler, McJSON, FPHashTable;

type
  // 신규: 확장 데이터 캐시 키 클래스 (제너릭 대신 사용)
  TCacheKey = class
  private
    FHostName: string;
    FIPAddress: string;
    FAppName: string;
    FUserName: string;
    FHash: Cardinal;
  public
    constructor Create(const AHostName, AIPAddress, AAppName, AUserName: string);
    function GetHashCode: Cardinal;
    function Equals(Obj: TObject): Boolean; override;
    property HostName: string read FHostName;
    property IPAddress: string read FIPAddress;
    property AppName: string read FAppName;
    property UserName: string read FUserName;
  end;

  { TDatabaseHandler - 데이터베이스 로그 핸들러 }
  TDatabaseHandler = class(TLogHandler)
  private
    FConnection: TZConnection;
    FHost: string;
    FDatabase: string;
    FUser: string;
    FPassword: string;
    FConnected: Boolean;
    FTablePrefix: string;
    FCurrentMonthTable: string;
    FMetaTableName: string;
    FExtDataTableName: string;  // 신규: 확장 데이터 테이블 이름
    FUseExtendedData: Boolean;  // 신규: 확장 데이터 사용 여부
    FExtDataCache: TFPObjectHashTable; // 신규: 확장 데이터 캐시 (제너릭 대신 사용)
    FExtDataCacheEnabled: Boolean; // 신규: 확장 데이터 캐시 활성화 여부

    // 데이터베이스 연결
    procedure Connect;
    procedure Disconnect;

    // 테이블 생성
    procedure CreateMetaTable;
    procedure CreateMonthlyTable(const TableName: string);
    procedure CreateExtDataTable; // 신규: 확장 데이터 테이블 생성

    // 현재 월 테이블 이름 생성
    function GenerateMonthTableName: string;

    // 신규: 확장 데이터 캐시에서 ID 찾기
    function FindExtDataIDInCache(const SourceInfo: TSourceInfo): Int64;
    // 신규: 확장 데이터 캐시에 ID 추가
    procedure AddExtDataIDToCache(const SourceInfo: TSourceInfo; ID: Int64);

  protected
    // 로그 핸들러 메서드 구현
    procedure DoInit; override;
    procedure DoShutdown; override;
    procedure DoDeliver(const Msg: string; Level: TLogLevel; const Tag: string); override;

  public
    constructor Create(const AHost, ADatabase, AUser, APassword: string);
    destructor Destroy; override;

    // 데이터베이스 연결 관리
    function IsConnected: Boolean;
    function Connect(const AHost, ADatabase, AUser, APassword: string): Boolean;
    procedure Reconnect;

    // 테이블 관리
    procedure CheckTables;
    procedure RotateTables;

    // 로그 쿼리
    function GetLogs(const StartDate, EndDate: TDateTime;
                    const LogLevel: string = '';
                    const SearchText: string = ''): TZQuery;

    // 신규: 확장 데이터 관련 메서드
    // 확장 데이터 저장
    function StoreExtendedData(const SourceInfo: TSourceInfo): Int64;
    // 서버 핸들러 데이터 저장
    function StoreServerHandlerData(const LogSourceInfo: TLogSourceInfo): Int64;
    // 확장 데이터 포함 로그 전달
    procedure DeliverWithExtData(const Msg: string; Level: TLogLevel; const Tag: string; ExtDataID: Int64);
    // 확장 데이터 포함 로그 조회
    function GetLogWithExtData(const StartDate, EndDate: TDateTime;
                              const LogLevel: string = '';
                              const SearchText: string = ''): TZQuery;
    // JSON 필드로 로그 조회
    function GetLogsByJsonField(const JsonPath, JsonValue: string): TZQuery;

    // 캐시 관리
    procedure ClearExtDataCache;
    procedure EnableExtDataCache(Enable: Boolean);

    // 속성
    property Connection: TZConnection read FConnection;
    property Connected: Boolean read FConnected;
    property TablePrefix: string read FTablePrefix write FTablePrefix;
    property CurrentMonthTable: string read FCurrentMonthTable;
    property MetaTableName: string read FMetaTableName;
    property ExtDataTableName: string read FExtDataTableName; // 신규: 확장 데이터 테이블 이름 속성
    property UseExtendedData: Boolean read FUseExtendedData write FUseExtendedData; // 신규: 확장 데이터 사용 여부 속성
  end;

implementation

{ TCacheKey }

constructor TCacheKey.Create(const AHostName, AIPAddress, AAppName, AUserName: string);
var
  HashStr: string;
begin
  FHostName := AHostName;
  FIPAddress := AIPAddress;
  FAppName := AAppName;
  FUserName := AUserName;

  // 해시 계산
  HashStr := FHostName + '|' + FIPAddress + '|' + FAppName + '|' + FUserName;
  FHash := 0;
  
  // 간단한 해시 함수 구현
  if HashStr <> '' then
  begin
    FHash := 5381;
    for var i := 1 to Length(HashStr) do
      FHash := ((FHash shl 5) + FHash) + Ord(HashStr[i]);
  end;
end;

function TCacheKey.GetHashCode: Cardinal;
begin
  Result := FHash;
end;

function TCacheKey.Equals(Obj: TObject): Boolean;
var
  Other: TCacheKey;
begin
  Result := False;
  if not (Obj is TCacheKey) then Exit;
  
  Other := TCacheKey(Obj);
  Result := (FHostName = Other.FHostName) and
           (FIPAddress = Other.FIPAddress) and
           (FAppName = Other.FAppName) and
           (FUserName = Other.FUserName);
end;

{ TDatabaseHandler }

constructor TDatabaseHandler.Create(const AHost, ADatabase, AUser, APassword: string);
begin
  inherited Create;

  // 데이터베이스 연결 정보 저장
  FHost := AHost;
  FDatabase := ADatabase;
  FUser := AUser;
  FPassword := APassword;
  FConnected := False;
  FTablePrefix := 'LOGS_';
  FMetaTableName := 'LOG_META';
  FExtDataTableName := 'LOG_EXT_DATA'; // 신규: 확장 데이터 테이블 이름 설정

  // 신규: 확장 데이터 관련 초기화
  FUseExtendedData := False;
  FExtDataCacheEnabled := True;
  FExtDataCache := TFPObjectHashTable.Create(True);
  
  // 현재 월 테이블 이름 생성
  FCurrentMonthTable := GenerateMonthTableName;

  // 연결 객체 생성
  FConnection := TZConnection.Create(nil);
  FConnection.Protocol := 'firebird-3.0';
  FConnection.LibraryLocation := '';
  FConnection.HostName := FHost;
  FConnection.Database := FDatabase;
  FConnection.User := FUser;
  FConnection.Password := FPassword;
  FConnection.LoginPrompt := False;
end;

destructor TDatabaseHandler.Destroy;
begin
  // 데이터베이스 연결 종료
  Disconnect;
  FConnection.Free;
  
  // 신규: 확장 데이터 캐시 정리
  FExtDataCache.Free;

  inherited;
end;

procedure TDatabaseHandler.DoInit;
begin
  // 데이터베이스 연결
  Connect;

  // 테이블 확인 및 생성
  CheckTables;
end;

procedure TDatabaseHandler.DoShutdown;
begin
  // 데이터베이스 연결 종료
  Disconnect;
end;

procedure TDatabaseHandler.DoDeliver(const Msg: string; Level: TLogLevel; const Tag: string);
var
  SQL: string;
  Query: TZQuery;
  LevelStr: string;
begin
  // 연결 확인
  if not FConnected then
  begin
    try
      Connect;
    except
      // 연결 실패 시 종료
      Exit;
    end;
  end;

  // 로그 레벨 문자열 변환
  case Level of
    llDevelop: LevelStr := 'DEVELOP';
    llDebug:   LevelStr := 'DEBUG';
    llInfo:    LevelStr := 'INFO';
    llWarning: LevelStr := 'WARNING';
    llError:   LevelStr := 'ERROR';
    llFatal:   LevelStr := 'FATAL';
  else
    LevelStr := 'INFO';
  end;

  try
    // 쿼리 생성
    Query := TZQuery.Create(nil);
    try
      Query.Connection := FConnection;
      
      // 테이블 이름 확인 (월이 바뀌었을 수 있음)
      if FCurrentMonthTable <> GenerateMonthTableName then
      begin
        FCurrentMonthTable := GenerateMonthTableName;
        CheckTables;
      end;

      // 기본 로그 저장 (확장 데이터 ID 없음)
      SQL := 'INSERT INTO ' + FCurrentMonthTable +
             ' (TIMESTAMP, LEVEL, MESSAGE, TAG, SOURCE_ID, EXT_DATA_ID) ' +
             'VALUES (:TIMESTAMP, :LEVEL, :MESSAGE, :TAG, :SOURCE_ID, NULL)';

      Query.SQL.Text := SQL;
      Query.ParamByName('TIMESTAMP').AsDateTime := Now;
      Query.ParamByName('LEVEL').AsString := LevelStr;
      Query.ParamByName('MESSAGE').AsString := Msg;
      
      if Tag <> '' then
        Query.ParamByName('TAG').AsString := Tag
      else
        Query.ParamByName('TAG').AsString := '';
        
      if SourceIdentifier <> '' then
        Query.ParamByName('SOURCE_ID').AsString := SourceIdentifier
      else
        Query.ParamByName('SOURCE_ID').AsString := '';
        
      Query.ExecSQL;
    finally
      Query.Free;
    end;
  except
    on E: Exception do
    begin
      // 오류 처리
      if Assigned(OnError) then
        OnError(Self, E.Message);
    end;
  end;
end;

// 신규: 확장 데이터 포함 로그 전달
procedure TDatabaseHandler.DeliverWithExtData(const Msg: string; Level: TLogLevel; const Tag: string; ExtDataID: Int64);
var
  SQL: string;
  Query: TZQuery;
  LevelStr: string;
begin
  // 연결 확인
  if not FConnected then
  begin
    try
      Connect;
    except
      // 연결 실패 시 종료
      Exit;
    end;
  end;

  // 확장 데이터 사용 안 함이면 기본 전달 사용
  if (not FUseExtendedData) or (ExtDataID <= 0) then
  begin
    DoDeliver(Msg, Level, Tag);
    Exit;
  end;

  // 로그 레벨 문자열 변환
  case Level of
    llDevelop: LevelStr := 'DEVELOP';
    llDebug:   LevelStr := 'DEBUG';
    llInfo:    LevelStr := 'INFO';
    llWarning: LevelStr := 'WARNING';
    llError:   LevelStr := 'ERROR';
    llFatal:   LevelStr := 'FATAL';
  else
    LevelStr := 'INFO';
  end;

  try
    // 쿼리 생성
    Query := TZQuery.Create(nil);
    try
      Query.Connection := FConnection;
      
      // 테이블 이름 확인 (월이 바뀌었을 수 있음)
      if FCurrentMonthTable <> GenerateMonthTableName then
      begin
        FCurrentMonthTable := GenerateMonthTableName;
        CheckTables;
      end;

      // 확장 데이터 ID 포함 로그 저장
      SQL := 'INSERT INTO ' + FCurrentMonthTable +
             ' (TIMESTAMP, LEVEL, MESSAGE, TAG, SOURCE_ID, EXT_DATA_ID) ' +
             'VALUES (:TIMESTAMP, :LEVEL, :MESSAGE, :TAG, :SOURCE_ID, :EXT_DATA_ID)';

      Query.SQL.Text := SQL;
      Query.ParamByName('TIMESTAMP').AsDateTime := Now;
      Query.ParamByName('LEVEL').AsString := LevelStr;
      Query.ParamByName('MESSAGE').AsString := Msg;
      
      if Tag <> '' then
        Query.ParamByName('TAG').AsString := Tag
      else
        Query.ParamByName('TAG').AsString := '';
        
      if SourceIdentifier <> '' then
        Query.ParamByName('SOURCE_ID').AsString := SourceIdentifier
      else
        Query.ParamByName('SOURCE_ID').AsString := '';
        
      Query.ParamByName('EXT_DATA_ID').AsInteger := ExtDataID;
      Query.ExecSQL;
    finally
      Query.Free;
    end;
  except
    on E: Exception do
    begin
      // 오류 처리
      if Assigned(OnError) then
        OnError(Self, E.Message);
    end;
  end;
end;

procedure TDatabaseHandler.Connect;
begin
  if FConnected then Exit;

  try
    FConnection.Connect;
    FConnected := True;
  except
    on E: Exception do
    begin
      FConnected := False;
      if Assigned(OnError) then
        OnError(Self, 'Database connection failed: ' + E.Message);
      raise;
    end;
  end;
end;

procedure TDatabaseHandler.Disconnect;
begin
  if not FConnected then Exit;

  try
    FConnection.Disconnect;
    FConnected := False;
  except
    on E: Exception do
    begin
      if Assigned(OnError) then
        OnError(Self, 'Database disconnect failed: ' + E.Message);
    end;
  end;
end;

function TDatabaseHandler.IsConnected: Boolean;
begin
  Result := FConnected and FConnection.Connected;
end;

function TDatabaseHandler.Connect(const AHost, ADatabase, AUser, APassword: string): Boolean;
begin
  // 기존 연결 종료
  Disconnect;

  // 새 연결 정보 설정
  FHost := AHost;
  FDatabase := ADatabase;
  FUser := AUser;
  FPassword := APassword;

  FConnection.HostName := FHost;
  FConnection.Database := FDatabase;
  FConnection.User := FUser;
  FConnection.Password := FPassword;

  // 연결 시도
  try
    Connect;
    Result := True;
  except
    Result := False;
  end;
end;

procedure TDatabaseHandler.Reconnect;
begin
  Disconnect;
  Connect;
end;

procedure TDatabaseHandler.CreateMetaTable;
var
  Query: TZQuery;
begin
  if not FConnected then Connect;

  Query := TZQuery.Create(nil);
  try
    Query.Connection := FConnection;

    // 메타 테이블 생성
    Query.SQL.Text := 'CREATE TABLE IF NOT EXISTS ' + FMetaTableName + ' (' +
                      'TABLE_NAME VARCHAR(50) NOT NULL PRIMARY KEY, ' +
                      'CREATED_DATE TIMESTAMP NOT NULL, ' +
                      'RECORD_COUNT INTEGER DEFAULT 0, ' +
                      'LAST_UPDATE TIMESTAMP)';
    Query.ExecSQL;
  finally
    Query.Free;
  end;
end;

procedure TDatabaseHandler.CreateMonthlyTable(const TableName: string);
var
  Query: TZQuery;
  SQL, MetaSQL: string;
begin
  if not FConnected then Connect;

  Query := TZQuery.Create(nil);
  try
    Query.Connection := FConnection;

    // 월별 로그 테이블 생성
    SQL := 'CREATE TABLE IF NOT EXISTS ' + TableName + ' (' +
           'LOG_ID INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ' +
           'TIMESTAMP TIMESTAMP NOT NULL, ' +
           'LEVEL VARCHAR(10) NOT NULL, ' +
           'MESSAGE VARCHAR(4000) NOT NULL, ' +
           'TAG VARCHAR(100), ' +
           'SOURCE_ID VARCHAR(100), ' +
           'EXT_DATA_ID INTEGER)'; // 신규: 확장 데이터 ID 필드 추가

    Query.SQL.Text := SQL;
    Query.ExecSQL;

    // 인덱스 생성
    SQL := 'CREATE INDEX IF NOT EXISTS IDX_' + TableName + '_TIMESTAMP ON ' + TableName + ' (TIMESTAMP)';
    Query.SQL.Text := SQL;
    Query.ExecSQL;

    SQL := 'CREATE INDEX IF NOT EXISTS IDX_' + TableName + '_LEVEL ON ' + TableName + ' (LEVEL)';
    Query.SQL.Text := SQL;
    Query.ExecSQL;

    SQL := 'CREATE INDEX IF NOT EXISTS IDX_' + TableName + '_TAG ON ' + TableName + ' (TAG)';
    Query.SQL.Text := SQL;
    Query.ExecSQL;

    // 신규: 확장 데이터 ID 인덱스 생성
    SQL := 'CREATE INDEX IF NOT EXISTS IDX_' + TableName + '_EXT_DATA_ID ON ' + TableName + ' (EXT_DATA_ID)';
    Query.SQL.Text := SQL;
    Query.ExecSQL;

    // 메타 테이블에 등록
    MetaSQL := 'INSERT INTO ' + FMetaTableName +
               ' (TABLE_NAME, CREATED_DATE, RECORD_COUNT, LAST_UPDATE) ' +
               'VALUES (:TABLE_NAME, :CREATED_DATE, 0, :LAST_UPDATE) ' +
               'ON CONFLICT(TABLE_NAME) DO UPDATE SET ' +
               'LAST_UPDATE = :LAST_UPDATE';

    Query.SQL.Text := MetaSQL;
    Query.ParamByName('TABLE_NAME').AsString := TableName;
    Query.ParamByName('CREATED_DATE').AsDateTime := Now;
    Query.ParamByName('LAST_UPDATE').AsDateTime := Now;
    Query.ExecSQL;
  finally
    Query.Free;
  end;
end;

// 신규: 확장 데이터 테이블 생성
procedure TDatabaseHandler.CreateExtDataTable;
var
  Query: TZQuery;
  SQL: string;
begin
  if not FConnected then Connect;

  Query := TZQuery.Create(nil);
  try
    Query.Connection := FConnection;

    // 확장 데이터 테이블 생성
    SQL := 'CREATE TABLE IF NOT EXISTS ' + FExtDataTableName + ' (' +
           'EXT_DATA_ID INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ' +
           'HOST_NAME VARCHAR(100), ' +
           'IP_ADDRESS VARCHAR(50), ' +
           'APP_NAME VARCHAR(100), ' +
           'APP_VERSION VARCHAR(50), ' +
           'USER_NAME VARCHAR(100), ' +
           'PROCESS_ID INTEGER, ' +
           'INSTANCE_ID VARCHAR(100), ' +
           'CREATED_DATE TIMESTAMP NOT NULL, ' +
           'EXTRA_DATA BLOB SUB_TYPE JSON)'; // Firebird 5.0의 JSON 지원 활용

    Query.SQL.Text := SQL;
    Query.ExecSQL;

    // 인덱스 생성
    SQL := 'CREATE INDEX IF NOT EXISTS IDX_' + FExtDataTableName + '_HOST ON ' + FExtDataTableName + ' (HOST_NAME)';
    Query.SQL.Text := SQL;
    Query.ExecSQL;

    SQL := 'CREATE INDEX IF NOT EXISTS IDX_' + FExtDataTableName + '_IP ON ' + FExtDataTableName + ' (IP_ADDRESS)';
    Query.SQL.Text := SQL;
    Query.ExecSQL;

    SQL := 'CREATE INDEX IF NOT EXISTS IDX_' + FExtDataTableName + '_APP ON ' + FExtDataTableName + ' (APP_NAME)';
    Query.SQL.Text := SQL;
    Query.ExecSQL;
  finally
    Query.Free;
  end;
end;

procedure TDatabaseHandler.CheckTables;
begin
  if not FConnected then Connect;

  // 메타 테이블 확인 및 생성
  CreateMetaTable;

  // 현재 월 테이블 확인 및 생성
  CreateMonthlyTable(FCurrentMonthTable);

  // 신규: 확장 데이터 테이블 확인 및 생성
  if FUseExtendedData then
    CreateExtDataTable;
end;

procedure TDatabaseHandler.RotateTables;
begin
  // 새 월 테이블 이름 생성
  FCurrentMonthTable := GenerateMonthTableName;

  // 테이블 확인 및 생성
  CheckTables;
end;

function TDatabaseHandler.GenerateMonthTableName: string;
begin
  // 현재 연월 기준 테이블 이름 생성 (LOGS_YYYYMM)
  Result := FTablePrefix + FormatDateTime('YYYYMM', Now);
end;

function TDatabaseHandler.GetLogs(const StartDate, EndDate: TDateTime;
                                 const LogLevel: string = '';
                                 const SearchText: string = ''): TZQuery;
var
  SQL: string;
  WhereClause: string;
begin
  if not FConnected then Connect;

  Result := TZQuery.Create(nil);
  Result.Connection := FConnection;

  // 기본 쿼리
  SQL := 'SELECT LOG_ID, TIMESTAMP, LEVEL, MESSAGE, TAG, SOURCE_ID, EXT_DATA_ID ' +
         'FROM ' + FCurrentMonthTable + ' ';

  // 조건절 구성
  WhereClause := 'WHERE TIMESTAMP BETWEEN :START_DATE AND :END_DATE ';

  // 로그 레벨 필터
  if LogLevel <> '' then
    WhereClause := WhereClause + 'AND LEVEL = :LOG_LEVEL ';

  // 검색어 필터
  if SearchText <> '' then
    WhereClause := WhereClause + 'AND (MESSAGE CONTAINING :SEARCH_TEXT OR TAG CONTAINING :SEARCH_TEXT) ';

  // 정렬
  SQL := SQL + WhereClause + 'ORDER BY TIMESTAMP DESC';

  Result.SQL.Text := SQL;
  Result.ParamByName('START_DATE').AsDateTime := StartDate;
  Result.ParamByName('END_DATE').AsDateTime := EndDate;

  if LogLevel <> '' then
    Result.ParamByName('LOG_LEVEL').AsString := LogLevel;

  if SearchText <> '' then
    Result.ParamByName('SEARCH_TEXT').AsString := SearchText;

  Result.Open;
end;

// 신규: 확장 데이터 저장
function TDatabaseHandler.StoreExtendedData(const SourceInfo: TSourceInfo): Int64;
var
  Query: TZQuery;
  SQL: string;
  JsonObj: TMcJsonObject;
  JsonStr: string;
  CachedID: Int64;
begin
  Result := 0;

  // 확장 데이터 사용 안 함이면 종료
  if not FUseExtendedData then Exit;

  // 연결 확인
  if not FConnected then
  begin
    try
      Connect;
    except
      Exit;
    end;
  end;

  // 캐시에서 확장 데이터 ID 찾기
  if FExtDataCacheEnabled then
  begin
    CachedID := FindExtDataIDInCache(SourceInfo);
    if CachedID > 0 then
    begin
      Result := CachedID;
      Exit;
    end;
  end;

  // 확장 데이터 테이블 확인
  CreateExtDataTable;

  // JSON 데이터 생성
  JsonObj := TMcJsonObject.Create;
  try
    // 추가 정보 저장
    JsonObj.AddValue('timestamp', FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now));
    JsonObj.AddValue('unique_id', SourceInfo.UniqueInstanceID);
    
    // JSON 문자열로 변환
    JsonStr := JsonObj.AsJson;
  finally
    JsonObj.Free;
  end;

  // 쿼리 생성
  Query := TZQuery.Create(nil);
  try
    Query.Connection := FConnection;

    // 확장 데이터 저장
    SQL := 'INSERT INTO ' + FExtDataTableName +
           ' (HOST_NAME, IP_ADDRESS, APP_NAME, APP_VERSION, USER_NAME, PROCESS_ID, INSTANCE_ID, CREATED_DATE, EXTRA_DATA) ' +
           'VALUES (:HOST_NAME, :IP_ADDRESS, :APP_NAME, :APP_VERSION, :USER_NAME, :PROCESS_ID, :INSTANCE_ID, :CREATED_DATE, :EXTRA_DATA) ' +
           'RETURNING EXT_DATA_ID';

    Query.SQL.Text := SQL;
    Query.ParamByName('HOST_NAME').AsString := SourceInfo.HostName;
    Query.ParamByName('IP_ADDRESS').AsString := SourceInfo.IPAddress;
    Query.ParamByName('APP_NAME').AsString := SourceInfo.ApplicationName;
    Query.ParamByName('APP_VERSION').AsString := SourceInfo.ApplicationVersion;
    Query.ParamByName('USER_NAME').AsString := SourceInfo.UserName;
    Query.ParamByName('PROCESS_ID').AsInteger := SourceInfo.ProcessID;
    Query.ParamByName('INSTANCE_ID').AsString := SourceInfo.UniqueInstanceID;
    Query.ParamByName('CREATED_DATE').AsDateTime := Now;
    Query.ParamByName('EXTRA_DATA').AsString := JsonStr;
    
    Query.Open;
    
    if not Query.IsEmpty then
    begin
      Result := Query.FieldByName('EXT_DATA_ID').AsInteger;
      
      // 캐시에 추가
      if FExtDataCacheEnabled and (Result > 0) then
        AddExtDataIDToCache(SourceInfo, Result);
    end;
  finally
    Query.Free;
  end;
end;

// 신규: 서버 핸들러 데이터 저장
function TDatabaseHandler.StoreServerHandlerData(const LogSourceInfo: TLogSourceInfo): Int64;
var
  Query: TZQuery;
  SQL: string;
  JsonObj: TMcJsonObject;
  JsonStr: string;
begin
  Result := 0;

  // 확장 데이터 사용 안 함이면 종료
  if not FUseExtendedData then Exit;

  // 연결 확인
  if not FConnected then
  begin
    try
      Connect;
    except
      Exit;
    end;
  end;

  // 확장 데이터 테이블 확인
  CreateExtDataTable;

  // JSON 데이터 생성
  JsonObj := TMcJsonObject.Create;
  try
    // 추가 정보 저장
    JsonObj.AddValue('timestamp', FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now));
    JsonObj.AddValue('client_ip', LogSourceInfo.ClientIP);
    JsonObj.AddValue('client_port', LogSourceInfo.ClientPort);
    JsonObj.AddValue('session_id', LogSourceInfo.SessionID);
    
    // JSON 문자열로 변환
    JsonStr := JsonObj.AsJson;
  finally
    JsonObj.Free;
  end;

  // 쿼리 생성
  Query := TZQuery.Create(nil);
  try
    Query.Connection := FConnection;

    // 확장 데이터 저장
    SQL := 'INSERT INTO ' + FExtDataTableName +
           ' (HOST_NAME, IP_ADDRESS, APP_NAME, APP_VERSION, USER_NAME, PROCESS_ID, INSTANCE_ID, CREATED_DATE, EXTRA_DATA) ' +
           'VALUES (:HOST_NAME, :IP_ADDRESS, :APP_NAME, :APP_VERSION, :USER_NAME, :PROCESS_ID, :INSTANCE_ID, :CREATED_DATE, :EXTRA_DATA) ' +
           'RETURNING EXT_DATA_ID';

    Query.SQL.Text := SQL;
    Query.ParamByName('HOST_NAME').AsString := LogSourceInfo.HostName;
    Query.ParamByName('IP_ADDRESS').AsString := LogSourceInfo.ClientIP;
    Query.ParamByName('APP_NAME').AsString := LogSourceInfo.AppName;
    Query.ParamByName('APP_VERSION').AsString := '';
    Query.ParamByName('USER_NAME').AsString := LogSourceInfo.UserName;
    Query.ParamByName('PROCESS_ID').AsInteger := 0;
    Query.ParamByName('INSTANCE_ID').AsString := LogSourceInfo.SessionID;
    Query.ParamByName('CREATED_DATE').AsDateTime := Now;
    Query.ParamByName('EXTRA_DATA').AsString := JsonStr;
    
    Query.Open;
    
    if not Query.IsEmpty then
      Result := Query.FieldByName('EXT_DATA_ID').AsInteger;
  finally
    Query.Free;
  end;
end;

// 신규: 확장 데이터 포함 로그 조회
function TDatabaseHandler.GetLogWithExtData(const StartDate, EndDate: TDateTime;
                                           const LogLevel: string = '';
                                           const SearchText: string = ''): TZQuery;
var
  SQL: string;
  WhereClause: string;
begin
  if not FConnected then Connect;

  Result := TZQuery.Create(nil);
  Result.Connection := FConnection;

  // 기본 쿼리 (로그 테이블과 확장 데이터 테이블 조인)
  SQL := 'SELECT l.LOG_ID, l.TIMESTAMP, l.LEVEL, l.MESSAGE, l.TAG, l.SOURCE_ID, ' +
         'e.HOST_NAME, e.IP_ADDRESS, e.APP_NAME, e.APP_VERSION, e.USER_NAME, e.PROCESS_ID, e.INSTANCE_ID, e.EXTRA_DATA ' +
         'FROM ' + FCurrentMonthTable + ' l ' +
         'LEFT JOIN ' + FExtDataTableName + ' e ON l.EXT_DATA_ID = e.EXT_DATA_ID ';

  // 조건절 구성
  WhereClause := 'WHERE l.TIMESTAMP BETWEEN :START_DATE AND :END_DATE ';

  // 로그 레벨 필터
  if LogLevel <> '' then
    WhereClause := WhereClause + 'AND l.LEVEL = :LOG_LEVEL ';

  // 검색어 필터
  if SearchText <> '' then
    WhereClause := WhereClause + 'AND (l.MESSAGE CONTAINING :SEARCH_TEXT OR l.TAG CONTAINING :SEARCH_TEXT) ';

  // 정렬
  SQL := SQL + WhereClause + 'ORDER BY l.TIMESTAMP DESC';

  Result.SQL.Text := SQL;
  Result.ParamByName('START_DATE').AsDateTime := StartDate;
  Result.ParamByName('END_DATE').AsDateTime := EndDate;

  if LogLevel <> '' then
    Result.ParamByName('LOG_LEVEL').AsString := LogLevel;

  if SearchText <> '' then
    Result.ParamByName('SEARCH_TEXT').AsString := SearchText;

  Result.Open;
end;

// 신규: JSON 필드로 로그 조회
function TDatabaseHandler.GetLogsByJsonField(const JsonPath, JsonValue: string): TZQuery;
var
  SQL: string;
begin
  if not FConnected then Connect;

  Result := TZQuery.Create(nil);
  Result.Connection := FConnection;

  // JSON 필드 필터링 쿼리
  SQL := 'SELECT l.LOG_ID, l.TIMESTAMP, l.LEVEL, l.MESSAGE, l.TAG, l.SOURCE_ID, ' +
         'e.HOST_NAME, e.IP_ADDRESS, e.APP_NAME, e.USER_NAME, e.EXTRA_DATA ' +
         'FROM ' + FCurrentMonthTable + ' l ' +
         'JOIN ' + FExtDataTableName + ' e ON l.EXT_DATA_ID = e.EXT_DATA_ID ' +
         'WHERE JSON_VALUE(e.EXTRA_DATA, :JSON_PATH) = :JSON_VALUE ' +
         'ORDER BY l.TIMESTAMP DESC';

  Result.SQL.Text := SQL;
  Result.ParamByName('JSON_PATH').AsString := JsonPath;
  Result.ParamByName('JSON_VALUE').AsString := JsonValue;
  Result.Open;
end;

// 신규: 확장 데이터 캐시 관리 메서드
procedure TDatabaseHandler.ClearExtDataCache;
begin
  FLock.Enter;
  try
    FExtDataCache.Clear;
  finally
    FLock.Leave;
  end;
end;

procedure TDatabaseHandler.EnableExtDataCache(Enable: Boolean);
begin
  FExtDataCacheEnabled := Enable;
  if not Enable then
    ClearExtDataCache;
end;

// 신규: 확장 데이터 캐시에서 ID 찾기
function TDatabaseHandler.FindExtDataIDInCache(const SourceInfo: TSourceInfo): Int64;
var
  Key: TCacheKey;
  Value: TObject;
begin
  Result := 0;
  
  Key := TCacheKey.Create(SourceInfo.HostName, SourceInfo.IPAddress, SourceInfo.ApplicationName, SourceInfo.UserName);
  try
    FLock.Enter;
    try
      Value := FExtDataCache.Items[Key.GetHashCode];
      if Assigned(Value) then
        Result := PtrInt(Value);
    finally
      FLock.Leave;
    end;
  finally
    Key.Free;
  end;
end;

// 신규: 확장 데이터 캐시에 ID 추가
procedure TDatabaseHandler.AddExtDataIDToCache(const SourceInfo: TSourceInfo; ID: Int64);
var
  Key: TCacheKey;
begin
  if ID <= 0 then Exit;
  
  Key := TCacheKey.Create(SourceInfo.HostName, SourceInfo.IPAddress, SourceInfo.ApplicationName, SourceInfo.UserName);
  
  FLock.Enter;
  try
    FExtDataCache.Add(Key, TObject(PtrInt(ID)));
  finally
    FLock.Leave;
  end;
end;

end.
