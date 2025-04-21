unit GlobalLogger;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, SyncObjs, DateUtils, Forms, StdCtrls,
  // 기본 로그 핸들러
  LogHandlers, FileHandler, MemoHandler, ConsoleHandler,
  // 확장 핸들러 모듈
  NetworkHandler, BufferedHandler, SerialHandler, PrintfHandler, DatabaseHandler,
  // 새로 통합된 모듈
  LogServerHandler, // 로그 서버 기능
  LogEmailHandler,  // 이메일 알림 기능
  SourceIdentifier, // 소스 식별자 기능
  // 시리얼 포트 지원을 위한 모듈
  CPort,
  // synapse 네트워크 라이브러리
  blcksock,
  // 신규: McJSON 라이브러리 추가
  McJSON;

type
  // 소스 식별자 형식 옵션
  TSourceDisplayMode = (
    sdmNone,        // 표시 안함
    sdmHandlerOnly, // 핸들러 소스만 표시
    sdmTagOnly,     // 태그만 표시
    sdmBoth,        // 핸들러 소스와 태그 모두 표시
    sdmFull         // 호스트, 앱, 핸들러 소스, 태그 모두 표시
  );

  // 소스 식별자 형식 설정
  TSourceFormatInfo = record
    DisplayMode: TSourceDisplayMode; // 표시 모드
    Prefix: string;                  // 전체 접두사
    Suffix: string;                  // 전체 접미사
    Separator: string;               // 구분자
    SourceFirst: Boolean;            // 소스 먼저 표시 여부 (False면 태그 먼저)
    UseColor: Boolean;               // 색상 사용 여부 (콘솔, 창 출력용)
  end;

  // 로그 형식 이벤트
  TLogFormatEvent = procedure(const Level: TLogLevel; const Msg: string;
    var FormattedMsg: string) of object;

  // 들여쓰기 관리 모드
  TIndentMode = (imNone, imAuto, imManual);

  // 신규: 확장 데이터 처리 모드
  TExtDataMode = (
    edmNone,     // 확장 데이터 사용 안함
    edmBasic,    // 기본 확장 데이터만 사용 (호스트명, IP, 앱 이름 등)
    edmFull,     // 모든 확장 데이터 사용 (JSON 포함)
    edmCustom    // 사용자 정의 확장 데이터 필드 사용
  );

  { TGlobalLogger - 메인 로거 클래스 }
  TGlobalLogger = class
  private
    FHandlers: TList;         // 로그 핸들러 목록
    FLock: TCriticalSection;  // 스레드 동기화 객체
    FOnFormatLog: TLogFormatEvent; // 로그 형식 이벤트
    FLogLevels: TLogLevelSet; // 로그 레벨 필터

    // 들여쓰기 관리 관련 필드
    FIndentMode: TIndentMode; // 들여쓰기 모드
    FIndentLevel: Integer;    // 현재 들여쓰기 레벨
    FIndentSize: Integer;     // 들여쓰기 크기
    FIndentChar: Char;        // 들여쓰기 문자

    // 타이밍 측정 관련 필드
    FTimings: TStringList;    // 타이밍 정보 저장용 리스트

    // 소스 식별자 관련 필드
    FSourceInfo: TSourceInfo;                         // 소스 정보 저장
    FUseSourceIdentifier: Boolean;                    // 소스 식별자 사용 여부
    FSourceIdentifierFormat: TSourceIdentifierFormat; // 소스 식별자 포맷
    FSourceCustomFormat: string;                      // 소스 식별자 사용자 정의 포맷
    FSourceFormat: TSourceFormatInfo;                 // 소스 식별자 형식 설정

    // 로그 서버 관련 필드
    FLogServerHandler: TLogServerHandler; // 로그 서버 핸들러
    FOnLogReceived: TLogReceiveEvent; // 로그 수신 이벤트

    // 이메일 알림 관련 필드
    FEmailHandler: TLogEmailHandler; // 이메일 핸들러
    // PrintfHandler 참조 필드
    FPrintfHandler: TPrintfHandler;

    // 시간 캐싱 변수
    FLastTimestamp: TDateTime; // 마지막으로 포맷팅된 시간
    FFormattedTimestamp: string; // 캐싱된 시간 문자열

    // 신규: 확장 데이터 관련 필드
    FExtDataMode: TExtDataMode;                      // 확장 데이터 처리 모드
    FDatabaseHandler: TDatabaseHandler;              // 데이터베이스 핸들러 참조
    FUseExtendedData: Boolean;                       // 확장 데이터 사용 여부
    FCustomExtDataFields: TStringList;               // 사용자 정의 확장 데이터 필드

    // 싱글턴 인스턴스
    class var FInstance: TGlobalLogger;

    // 로그 메시지 포맷팅
    function FormatLogMessage(const Level: TLogLevel; const Msg: string; const Tag: string = '';
                              Handler: TLogHandler = nil): string;

    // 들여쓰기 문자열 생성
    function GetIndentString: string;

    // 소스 식별자 문자열 생성
    function GetSourceIdentifierString: string;

    // 소스 정보 수집
    procedure CollectSourceInfo;

    // 시간 문자열 얻기 (캐싱 지원)
    function GetFormattedTimestamp: string;

    // 신규: 확장 데이터 JSON 생성
    function BuildExtendedDataJson(const Handler: TLogHandler; const Tag: string): string;
    // 신규: 확장 데이터 ID 가져오기
    function GetExtendedDataID(const Handler: TLogHandler; const Tag: string): Int64;

  public
    constructor Create;
    destructor Destroy; override;

    // 싱글턴 인스턴스 접근
    class function GetInstance: TGlobalLogger;
    class procedure FreeInstance;

    // 핸들러 관리
    procedure AddHandler(Handler: TLogHandler);
    procedure RemoveHandler(Handler: TLogHandler);
    function FindHandler(HandlerClass: TClass): TLogHandler;
    procedure ClearHandlers;

    // 버퍼링 핸들러
    function WrapWithBuffer(Handler: TLogHandler; BufferSize: Integer = 100): TBufferedHandler;
    // 확장된 RemoveHandler
    procedure RemoveHandler(Handler: TLogHandler; FreeHandler: Boolean = True);
    // 모든 버퍼 플러시
    procedure FlushAllBuffers;
    // 버퍼링 활성화
    procedure EnableBuffering(HandlerClass: TClass = nil; BufferSize: Integer = 100);
    // 네트워크 로깅 활성화
    procedure EnableNetworkLogging(const Host: string; Port: Integer; Buffered: Boolean = True);
    // 직렬 포트 로깅 활성화
    procedure EnableSerialLogging(const PortName: string; BaudRate: TBaudRate = br9600; Buffered: Boolean = True);

    // PrintfHandler 추가
    function AddPrintfHandler: TPrintfHandler;

    // PrintfWindow 활성화 (간편 메서드)
    procedure EnablePrintfWindow(const Caption: string; Width, Height: Integer;
                                 Position: TFormPosition;
                                 AutoRotate: Boolean = False;
                                 RotateLines: Integer = 5000);

    procedure ConfigurePrintfRotation(AutoRotate: Boolean; RotateLines: Integer;
                                      SaveBeforeClear: Boolean = True;
                                      const LogFolder: string = '');

    // 소스 식별자 래핑
    function WrapWithSourceIdentifier(Handler: TLogHandler; OwnsHandler: Boolean = True): TSourceIdentifierHandler;
    // 소스 식별자 활성화
    procedure EnableSourceIdentifier(Format: TSourceIdentifierFormat = sfHostAndApp; const CustomFormat: string = '');
    // 소스 정보 설정 및 조회
    procedure SetSourceInfo(const HostName, IPAddress, AppName, AppVersion, UserName: string);
    function GetSourceInfo: TSourceInfo;
    // 소스 식별자 형식 설정
    procedure ConfigureSourceFormat(DisplayMode: TSourceDisplayMode = sdmBoth;
                                  const Prefix: string = '[';
                                  const Suffix: string = ']';
                                  const Separator: string = ':';
                                  SourceFirst: Boolean = True);
    // 핸들러 소스 식별자 설정
    procedure SetHandlerSourceId(Handler: TLogHandler; const SourceId: string);
    // 모든 핸들러 소스 식별자 자동 설정
    procedure AutoConfigureHandlerSources;


    // 로그 서버 기능
    function AddLogServerHandler(Port: Integer = 9000): TLogServerHandler;
    procedure StartLogServer(Port: Integer = 9000);
    procedure StopLogServer;
    // IP 허용 목록 관리
    procedure AddAllowedServerIP(const IPAddress: string);
    procedure ClearAllowedServerIPs;

    // 이메일 핸들러 기능
    function AddEmailHandler: TLogEmailHandler;
    // SMTP 설정
    procedure ConfigureEmailSettings(const SmtpServer: string; SmtpPort: Integer;
                                     const Username, Password: string; UseSSL: Boolean = False);
    // 이메일 주소 설정
    procedure SetEmailAddresses(const FromAddr, ToAddrs, CcAddrs: string);
    // 이메일 필터링
    procedure SetEmailLogLevels(LogLevels: TLogLevelSet);


    // 기본 로깅 메서드
    procedure Log(const Msg: string; Level: TLogLevel = llInfo; const Tag: string = '');
    procedure LogFmt(const Fmt: string; const Args: array of const; Level: TLogLevel = llInfo; const Tag: string = '');
    procedure LogDevelop(const Msg: string; const Tag: string = '');
    procedure LogDebug(const Msg: string; const Tag: string = '');
    procedure LogInfo(const Msg: string; const Tag: string = '');
    procedure LogWarning(const Msg: string; const Tag: string = '');
    procedure LogError(const Msg: string; const Tag: string = '');
    procedure LogFatal(const Msg: string; const Tag: string = '');
    procedure LogException(const Msg: string; E: Exception; Level: TLogLevel = llError; const Tag: string = '');

    // 편의 메서드 - MultiLog 호환
    procedure Send(const Msg: string);
    procedure SendError(const Msg: string);
    procedure SendException(const Msg: string; E: Exception);

    // 핸들러 생성 편의 함수
    function AddFileHandler(const LogPath, FilePrefix: string): TFileHandler;
    function AddMemoHandler(AMemo: TCustomMemo): TMemoHandler;
    function AddConsoleHandler: TConsoleHandler;
    function AddNetworkHandler(const Host: string; Port: Integer): TNetworkHandler;
    function AddSerialHandler(ASerialPort: TComPort = nil; OwnsPort: Boolean = True): TSerialHandler; // 시리얼 핸들러
    function AddDatabaseHandler(const AHost, ADatabase, AUser, APassword: string): TDatabaseHandler;

    // 초기화 메서드 - 이전의 Configure와 유사한 기능
    procedure Configure(const LogPath, FilePrefix: string;
                       AMemo: TCustomMemo = nil;
                       ALogLevels: TLogLevelSet = [llDevelop..llFatal]);

    // 세션 시작 메서드
    procedure StartLogSession;

    // 들여쓰기 관리 메서드
    procedure IncreaseIndent;
    procedure DecreaseIndent;
    procedure ResetIndent;
    procedure LogWithIndent(const Msg: string; Level: TLogLevel = llInfo; const Tag: string = '');
    procedure LogFunctionEnter(const FuncName: string);
    procedure LogFunctionExit(const FuncName: string);

    // 타이밍 측정 메서드
    procedure StartTiming(const TimingLabel: string);
    procedure EndTiming(const TimingLabel: string; Level: TLogLevel = llInfo);
    function GetElapsedTime(const TimingLabel: string): Int64;  // 밀리초 단위
    procedure ClearTimings;

    // 필터링 메서드
    procedure EnableFilter(const FilterText: string; CaseSensitive: Boolean = False);
    procedure DisableFilter;

    // 태그 관련 메서드
    procedure LogWithTag(const Tag, Msg: string; Level: TLogLevel = llInfo);
    procedure LogFmtWithTag(const Tag, Fmt: string; const Args: array of const; Level: TLogLevel = llInfo);
    procedure EnableTagFilter(const Tags: array of string);
    procedure DisableTagFilter;

    // 비동기 처리 설정
    procedure EnableAsyncLogging(Enable: Boolean = True);

    // 신규: 확장 데이터 관련 메서드
    // 확장 데이터 모드 설정
    procedure SetExtendedDataMode(Mode: TExtDataMode);
    // 데이터베이스에 확장 데이터 저장 활성화
    procedure EnableExtendedDataStorage(Enable: Boolean = True);
    // 사용자 정의 확장 데이터 필드 추가
    procedure AddCustomExtDataField(const FieldName: string);
    // 사용자 정의 확장 데이터 필드 제거
    procedure RemoveCustomExtDataField(const FieldName: string);
    // 사용자 정의 확장 데이터 필드 지우기
    procedure ClearCustomExtDataFields;
    // 확장 데이터 포함 로그 조회
    function GetLogsWithExtData(const StartDate, EndDate: TDateTime;
                               const LogLevel: string = '';
                               const SearchText: string = ''): TZQuery;
    // JSON 필드 필터링으로 로그 조회
    function GetLogsByJsonFilter(const JsonPath, JsonValue: string): TZQuery;

    // 속성
    property OnFormatLog: TLogFormatEvent read FOnFormatLog write FOnFormatLog;
    property LogLevels: TLogLevelSet read FLogLevels write FLogLevels;
    property IndentMode: TIndentMode read FIndentMode write FIndentMode;
    property IndentLevel: Integer read FIndentLevel;
    property IndentSize: Integer read FIndentSize write FIndentSize;
    property IndentChar: Char read FIndentChar write FIndentChar;

    // 소스 식별자 관련 속성
    property UseSourceIdentifier: Boolean read FUseSourceIdentifier write FUseSourceIdentifier;
    property SourceIdentifierFormat: TSourceIdentifierFormat read FSourceIdentifierFormat write FSourceIdentifierFormat;
    property SourceCustomFormat: string read FSourceCustomFormat write FSourceCustomFormat;
    property SourceFormat: TSourceFormatInfo read FSourceFormat write FSourceFormat;

    // 로그 서버 관련 속성
    property OnLogReceived: TLogReceiveEvent read FOnLogReceived write FOnLogReceived;

    // 신규: 확장 데이터 관련 속성
    property ExtDataMode: TExtDataMode read FExtDataMode write SetExtendedDataMode;
    property UseExtendedData: Boolean read FUseExtendedData write EnableExtendedDataStorage;
  end;

// 간편한 전역 액세스 함수
function Logger: TGlobalLogger;
function GetPrintfHandler: TPrintfHandler;
function GetDatabaseHandler: TDatabaseHandler;
// 다른 유닛에서 접근할 수 있도록 interface 섹션에 함수 선언
procedure DebugToFile(const Msg: string);

implementation

{$IFDEF MSWINDOWS}
uses
  Windows;
{$ENDIF}

{$IFDEF UNIX}
uses
  BaseUnix, Unix, NetDB;
{$ENDIF}

// 타이밍 정보 구조체
type
  TTimingInfo = record
    TimingLabel: string;  // 이전의 Label 대신 TimingLabel로 변경
    StartTime: TDateTime;
  end;
  PTimingInfo = ^TTimingInfo;

var
  GlobalLoggerInstance: TGlobalLogger = nil;

procedure DebugToFile(const Msg: string);
var
  F: TextFile;
begin
  AssignFile(F, 'debug.log');
  try
    if FileExists('debug.log') then
      Append(F)
    else
      Rewrite(F);
    WriteLn(F, FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now) + ': ' + Msg);
  finally
    CloseFile(F);
  end;
end;

function Logger: TGlobalLogger;
begin
  Result := TGlobalLogger.GetInstance;
end;

function GetPrintfHandler: TPrintfHandler;
var
  i: Integer;
  Handler: TLogHandler;
  HandlerList: TList;
begin
  Result := nil;

  // Logger의 내부 핸들러 리스트에 접근
  HandlerList := Logger.FHandlers; // 또는 Logger.GetHandlers 등 적절한 메서드 사용

  if Assigned(HandlerList) then
  begin
    for i := 0 to HandlerList.Count - 1 do
    begin
      Handler := TLogHandler(HandlerList[i]);
      if Handler is TPrintfHandler then
      begin
        Result := TPrintfHandler(Handler);
        Break;
      end;
    end;
  end;
end;

function GetDatabaseHandler: TDatabaseHandler;
var
  i: Integer;
  Handler: TLogHandler;
  HandlerList: TList;
begin
  Result := nil;

  // Logger의 내부 핸들러 리스트에 접근
  HandlerList := Logger.FHandlers; // 또는 Logger.GetHandlers 등 적절한 메서드 사용

  if Assigned(HandlerList) then
  begin
    for i := 0 to HandlerList.Count - 1 do
    begin
      Handler := TLogHandler(HandlerList[i]);
      if Handler is TDatabaseHandler then
      begin
        Result := TDatabaseHandler(Handler);
        Break;
      end;
    end;
  end;
end;


{ TGlobalLogger }

constructor TGlobalLogger.Create;
begin
  inherited Create;

  // 기본 초기화
  FLock := SyncObjs.TCriticalSection.Create;
  FHandlers := TList.Create;
  FLogLevels := [llDevelop, llDebug, llInfo, llWarning, llError, llFatal];

  // 들여쓰기 초기화
  FIndentMode := imAuto;
  FIndentLevel := 0;
  FIndentSize := 2;
  FIndentChar := ' ';

  // 타이밍 초기화
  FTimings := TStringList.Create;

  // 소스 식별자 초기화
  FUseSourceIdentifier := False;
  FSourceIdentifierFormat := sfHostAndApp;
  FSourceCustomFormat := '[%HOST%][%APP%]';
  FillChar(FSourceInfo, SizeOf(FSourceInfo), 0);

  // 소스 식별자 형식 기본값 설정
  FSourceFormat.DisplayMode := sdmBoth;
  FSourceFormat.Prefix := '[';
  FSourceFormat.Suffix := ']';
  FSourceFormat.Separator := ':';
  FSourceFormat.SourceFirst := True;
  FSourceFormat.UseColor := False;

  // 로그 서버 핸들러 초기화
  FLogServerHandler := nil;

  // 이메일 핸들러 초기화
  FEmailHandler := nil;
  // Printf 핸들러 초기화
  FPrintfHandler := nil;

  // 시간 캐싱 초기화
  FLastTimestamp := 0;
  FFormattedTimestamp := '';

  // 신규: 확장 데이터 초기화
  FExtDataMode := edmNone;
  FDatabaseHandler := nil;
  FUseExtendedData := False;
  FCustomExtDataFields := TStringList.Create;

  // 소스 정보 수집
  CollectSourceInfo;
end;

destructor TGlobalLogger.Destroy;
begin
  // 핸들러 정리
  ClearHandlers;
  FHandlers.Free;

  // 타이밍 정리
  ClearTimings;
  FTimings.Free;

  // 특수 핸들러 레퍼런스 명시적 정리
  FLogServerHandler := nil;
  FEmailHandler := nil;
  FPrintfHandler := nil; // PrintfHandler 참조 정리
  FDatabaseHandler := nil; // 신규: DatabaseHandler 참조 정리

  // 신규: 사용자 정의 확장 데이터 필드 정리
  FCustomExtDataFields.Free;

  // 동기화 객체 정리
  FLock.Free;

  inherited;
end;

class function TGlobalLogger.GetInstance: TGlobalLogger;
begin
  if FInstance = nil then
    FInstance := TGlobalLogger.Create;
  Result := FInstance;
end;

class procedure TGlobalLogger.FreeInstance;
begin
  if FInstance <> nil then
  begin
    FInstance.Free;
    FInstance := nil;
  end;
end;

procedure TGlobalLogger.CollectSourceInfo;
var
  Socket: TTCPBlockSocket;
  {$IFDEF MSWINDOWS}
  Buffer: array[0..255] of Char;
  Len: DWORD;
  Guid: TGUID;
  {$ENDIF}
  {$IFDEF UNIX}
  Buffer: array[0..255] of Char;
  Info: utsname;
  Passwd: PPasswordRecord;
  {$ENDIF}
begin
  // 기본값 초기화
  FSourceInfo.HostName := 'unknown-host';
  FSourceInfo.IPAddress := '127.0.0.1';
  FSourceInfo.UserName := 'unknown-user';
  FSourceInfo.ApplicationName := ExtractFileName(ParamStr(0));
  FSourceInfo.ApplicationVersion := '1.0.0';

  try
    // Synapse를 사용하여 호스트명과 IP 주소 가져오기
    Socket := TTCPBlockSocket.Create;
    try
      try
        // 호스트 이름 가져오기
        FSourceInfo.HostName := Socket.LocalName;
        if FSourceInfo.HostName = '' then
          FSourceInfo.HostName := 'unknown-host';

        // IP 주소 가져오기
        FSourceInfo.IPAddress := Socket.ResolveName(FSourceInfo.HostName);
        if FSourceInfo.IPAddress = '' then
          FSourceInfo.IPAddress := '127.0.0.1';
      except
        FSourceInfo.HostName := 'unknown-host';
        FSourceInfo.IPAddress := '127.0.0.1';
      end;
    finally
      Socket.Free;
    end;

    {$IFDEF MSWINDOWS}
    // 사용자 이름 가져오기
    FillChar(Buffer, SizeOf(Buffer), 0);
    Len := GetEnvironmentVariable('USERNAME', Buffer, SizeOf(Buffer));
    if Len > 0 then
      FSourceInfo.UserName := Buffer
    else
      FSourceInfo.UserName := 'unknown-user';

    // 프로세스 ID 설정
    FSourceInfo.ProcessID := GetCurrentProcessId;

    // 고유 식별자 생성
    if CreateGUID(Guid) = S_OK then
      FSourceInfo.UniqueInstanceID := GUIDToString(Guid)
    else begin
      Randomize;
      FSourceInfo.UniqueInstanceID := Format('%s-%d-%d',
        [FormatDateTime('yyyymmddhhnnsszzz', Now),
         FSourceInfo.ProcessID,
         Random(1000000)]);
    end;
    {$ENDIF}

    {$IFDEF UNIX}
    // Unix 시스템 정보
    if fpUname(Info) = 0 then
      FSourceInfo.HostName := Info.nodename;

    // 사용자 이름
    FSourceInfo.UserName := GetEnvironmentVariable('USER');
    if FSourceInfo.UserName = '' then begin
      Passwd := getpwuid(fpGetUID);
      if Passwd <> nil then
        FSourceInfo.UserName := Passwd^.pw_name
      else
        FSourceInfo.UserName := 'unknown-user';
    end;

    // 프로세스 ID
    FSourceInfo.ProcessID := fpGetPID;

    // 고유 식별자 생성
    Randomize;
    FSourceInfo.UniqueInstanceID := Format('%s-%d-%d',
      [FormatDateTime('yyyymmddhhnnsszzz', Now),
       FSourceInfo.ProcessID,
       Random(1000000)]);
    {$ENDIF}
  except
    on E: Exception do
    begin
      // 소스 정보 수집 중 예외 발생 시 기본값 유지
      // 로그는 아직 설정되지 않았으므로 콘솔에 출력
      WriteLn('소스 정보 수집 중 오류: ', E.Message);
    end;
  end;
end;

function TGlobalLogger.GetFormattedTimestamp: string;
var
  CurrentTime: TDateTime;
begin
  CurrentTime := Now;

  // 캐시된 시간이 1초 이내면 재사용
  if (CurrentTime - FLastTimestamp) < (1 / (24 * 60 * 60)) then
    Result := FFormattedTimestamp
  else
  begin
    // 새로 포맷팅
    FLastTimestamp := CurrentTime;
    FFormattedTimestamp := FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', CurrentTime);
    Result := FFormattedTimestamp;
  end;
end;

// 신규: 확장 데이터 JSON 생성
function TGlobalLogger.BuildExtendedDataJson(const Handler: TLogHandler; const Tag: string): string;
var
  JsonObj: TMcJsonObject;
  SourceInfo: TSourceInfo;
  SourceHandler: TSourceIdentifierHandler;
  i: Integer;
begin
  JsonObj := TMcJsonObject.Create;
  try
    // 기본 정보 추가
    if Handler <> nil then
      JsonObj.AddValue('handler_type', Handler.ClassName);

    if Tag <> '' then
      JsonObj.AddValue('tag', Tag);

    // 소스 정보 추가
    if Handler is TSourceIdentifierHandler then
    begin
      SourceHandler := TSourceIdentifierHandler(Handler);
      SourceInfo := SourceHandler.SourceInfo;
    end
    else
      SourceInfo := FSourceInfo;

    // 확장 데이터 모드에 따라 필드 추가
    case FExtDataMode of
      edmBasic:
        begin
          JsonObj.AddValue('host_name', SourceInfo.HostName);
          JsonObj.AddValue('ip_address', SourceInfo.IPAddress);
          JsonObj.AddValue('application', SourceInfo.ApplicationName);
        end;

      edmFull:
        begin
          JsonObj.AddValue('host_name', SourceInfo.HostName);
          JsonObj.AddValue('ip_address', SourceInfo.IPAddress);
          JsonObj.AddValue('application', SourceInfo.ApplicationName);
          JsonObj.AddValue('app_version', SourceInfo.ApplicationVersion);
          JsonObj.AddValue('user_name', SourceInfo.UserName);
          JsonObj.AddValue('process_id', SourceInfo.ProcessID);
          JsonObj.AddValue('instance_id', SourceInfo.UniqueInstanceID);
          JsonObj.AddValue('timestamp', FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now));
        end;

      edmCustom:
        begin
          // 사용자 정의 필드 추가
          for i := 0 to FCustomExtDataFields.Count - 1 do
          begin
            if FCustomExtDataFields[i] = 'host_name' then
              JsonObj.AddValue('host_name', SourceInfo.HostName)
            else if FCustomExtDataFields[i] = 'ip_address' then
              JsonObj.AddValue('ip_address', SourceInfo.IPAddress)
            else if FCustomExtDataFields[i] = 'application' then
              JsonObj.AddValue('application', SourceInfo.ApplicationName)
            else if FCustomExtDataFields[i] = 'app_version' then
              JsonObj.AddValue('app_version', SourceInfo.ApplicationVersion)
            else if FCustomExtDataFields[i] = 'user_name' then
              JsonObj.AddValue('user_name', SourceInfo.UserName)
            else if FCustomExtDataFields[i] = 'process_id' then
              JsonObj.AddValue('process_id', SourceInfo.ProcessID)
            else if FCustomExtDataFields[i] = 'instance_id' then
              JsonObj.AddValue('instance_id', SourceInfo.UniqueInstanceID)
            else if FCustomExtDataFields[i] = 'timestamp' then
              JsonObj.AddValue('timestamp', FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now));
          end;
        end;
    end;

    // JSON 문자열로 변환
    Result := JsonObj.AsJson;
  finally
    JsonObj.Free;
  end;
end;

// 신규: 확장 데이터 ID 가져오기
function TGlobalLogger.GetExtendedDataID(const Handler: TLogHandler; const Tag: string): Int64;
var
  JsonData: string;
begin
  Result := 0;

  // 확장 데이터 모드가 None이면 종료
  if FExtDataMode = edmNone then
    Exit;

  // 데이터베이스 핸들러가 없으면 종료
  if FDatabaseHandler = nil then
  begin
    FDatabaseHandler := TDatabaseHandler(FindHandler(TDatabaseHandler));
    if FDatabaseHandler = nil then
      Exit;
  end;

  // 확장 데이터 저장이 비활성화되어 있으면 종료
  if not FUseExtendedData then
    Exit;

  // JSON 데이터 생성
  JsonData := BuildExtendedDataJson(Handler, Tag);

  // 데이터베이스 핸들러를 통해 확장 데이터 저장
  if Handler is TSourceIdentifierHandler then
    Result := FDatabaseHandler.StoreExtendedData(TSourceIdentifierHandler(Handler).SourceInfo)
  else if (Handler <> nil) and (Handler.ClassType.ClassName = 'TLogServerHandler') then
  begin
    // LogServerHandler의 경우 LogSourceInfo 정보 가져오기
    // 실제 구현에서는 적절한 방법으로 LogSourceInfo 설정 필요
    Result := FDatabaseHandler.StoreServerHandlerData(TLogServerHandler(Handler).SourceInfo);
  end
  else
  begin
    // 기타 핸들러의 경우 JSON 데이터만 저장
    Result := 0; // 실제 구현에서는 적절한 값으로 대체
  end;
end;

function TGlobalLogger.FormatLogMessage(const Level: TLogLevel; const Msg: string; const Tag: string = '';
                                       Handler: TLogHandler = nil): string;
var
  LevelStr: string;
  FormattedMsg: string;
  TagStr: string;
  SourceIdStr: string;
  TimeStr: string;
  SourceIdentifier: string;
begin
  // 로그 레벨 문자열 변환
  case Level of
    llDevelop:  LevelStr := 'DEVELOP';
    llDebug:    LevelStr := 'DEBUG';
    llInfo:     LevelStr := 'INFO';
    llWarning:  LevelStr := 'WARNING';
    llError:    LevelStr := 'ERROR';
    llFatal:    LevelStr := 'FATAL';
  end;

  // 시간 문자열 (캐싱 사용)
  TimeStr := GetFormattedTimestamp;

  // 소스 식별자 문자열 생성
  case FSourceFormat.DisplayMode of
    sdmNone:
      SourceIdStr := '';

    sdmHandlerOnly:
      if Assigned(Handler) and (Handler.SourceIdentifier <> '') then
        SourceIdStr := FSourceFormat.Prefix + Handler.SourceIdentifier + FSourceFormat.Suffix + ' '
      else
        SourceIdStr := '';

    sdmTagOnly:
      if Tag <> '' then
        SourceIdStr := FSourceFormat.Prefix + Tag + FSourceFormat.Suffix + ' '
      else
        SourceIdStr := '';

    sdmBoth:
      begin
        if (Tag <> '') and Assigned(Handler) and (Handler.SourceIdentifier <> '') then
        begin
          // 소스와 태그 모두 있는 경우
          if FSourceFormat.SourceFirst then
            SourceIdStr := FSourceFormat.Prefix + Handler.SourceIdentifier +
                          FSourceFormat.Separator + Tag + FSourceFormat.Suffix + ' '
          else
            SourceIdStr := FSourceFormat.Prefix + Tag +
                          FSourceFormat.Separator + Handler.SourceIdentifier + FSourceFormat.Suffix + ' ';
        end
        else if Tag <> '' then
          // 태그만 있는 경우
          SourceIdStr := FSourceFormat.Prefix + Tag + FSourceFormat.Suffix + ' '
        else if Assigned(Handler) and (Handler.SourceIdentifier <> '') then
          // 소스만 있는 경우
          SourceIdStr := FSourceFormat.Prefix + Handler.SourceIdentifier + FSourceFormat.Suffix + ' '
        else
          SourceIdStr := '';
      end;

    sdmFull:
      begin
        if FUseSourceIdentifier then
          // 호스트/앱 식별자
          SourceIdentifier := GetSourceIdentifierString
        else
          SourceIdentifier := '';

        // 태그 및 핸들러 소스 처리
        if (Tag <> '') or (Assigned(Handler) and (Handler.SourceIdentifier <> '')) then
        begin
          if Tag <> '' then
            TagStr := Tag
          else
            TagStr := '';

          if Assigned(Handler) and (Handler.SourceIdentifier <> '') then
            SourceIdStr := Handler.SourceIdentifier
          else
            SourceIdStr := '';

          // 소스와 태그 조합
          if (TagStr <> '') and (SourceIdStr <> '') then
          begin
            if FSourceFormat.SourceFirst then
              SourceIdStr := FSourceFormat.Prefix + SourceIdStr +
                            FSourceFormat.Separator + TagStr + FSourceFormat.Suffix
            else
              SourceIdStr := FSourceFormat.Prefix + TagStr +
                            FSourceFormat.Separator + SourceIdStr + FSourceFormat.Suffix;
          end
          else if TagStr <> '' then
            SourceIdStr := FSourceFormat.Prefix + TagStr + FSourceFormat.Suffix
          else if SourceIdStr <> '' then
            SourceIdStr := FSourceFormat.Prefix + SourceIdStr + FSourceFormat.Suffix;

          // 호스트/앱 식별자와 결합
          if SourceIdentifier <> '' then
            SourceIdStr := SourceIdentifier + ' ' + SourceIdStr
        end
        else
          SourceIdStr := SourceIdentifier;

        if SourceIdStr <> '' then
          SourceIdStr := SourceIdStr + ' ';
      end;
  end;

  // 들여쓰기 처리
  if FIndentMode <> imNone then
    FormattedMsg := Format('[%s] [%s] %s%s%s',
                         [TimeStr,
                          LevelStr,
                          SourceIdStr,
                          GetIndentString(),
                          Msg])
  else
    FormattedMsg := Format('[%s] [%s] %s%s',
                         [TimeStr,
                          LevelStr,
                          SourceIdStr,
                          Msg]);

  // 사용자 정의 형식이 있는 경우 적용
  if Assigned(FOnFormatLog) then
    FOnFormatLog(Level, Msg, FormattedMsg);

  Result := FormattedMsg;
end;


function TGlobalLogger.GetSourceIdentifierString: string;
var
  TempStr: string;
begin
  if not FUseSourceIdentifier then
  begin
    Result := '';
    Exit;
  end;

  // 포맷에 따라 소스 식별자 문자열 생성
  case FSourceIdentifierFormat of
    sfHostName:
      Result := Format('[%s]', [FSourceInfo.HostName]);

    sfIPAddress:
      Result := Format('[%s]', [FSourceInfo.IPAddress]);

    sfApplication:
      Result := Format('[%s %s]', [FSourceInfo.ApplicationName, FSourceInfo.ApplicationVersion]);

    sfHostAndApp:
      Result := Format('[%s][%s]', [FSourceInfo.HostName, FSourceInfo.ApplicationName]);

    sfIPAndApp:
      Result := Format('[%s][%s]', [FSourceInfo.IPAddress, FSourceInfo.ApplicationName]);

    sfFull:
      Result := Format('[%s][%s][%s %s][%s][%d]',
                       [FSourceInfo.HostName,
                        FSourceInfo.IPAddress,
                        FSourceInfo.ApplicationName,
                        FSourceInfo.ApplicationVersion,
                        FSourceInfo.UserName,
                        FSourceInfo.ProcessID]);

    sfCustom:
      begin
        TempStr := FSourceCustomFormat;

        // 포맷 문자열 내 변수 치환
        TempStr := StringReplace(TempStr, '%HOST%', FSourceInfo.HostName, [rfReplaceAll, rfIgnoreCase]);
        TempStr := StringReplace(TempStr, '%IP%', FSourceInfo.IPAddress, [rfReplaceAll, rfIgnoreCase]);
        TempStr := StringReplace(TempStr, '%APP%', FSourceInfo.ApplicationName, [rfReplaceAll, rfIgnoreCase]);
        TempStr := StringReplace(TempStr, '%VER%', FSourceInfo.ApplicationVersion, [rfReplaceAll, rfIgnoreCase]);
        TempStr := StringReplace(TempStr, '%USER%', FSourceInfo.UserName, [rfReplaceAll, rfIgnoreCase]);
        TempStr := StringReplace(TempStr, '%PID%', IntToStr(FSourceInfo.ProcessID), [rfReplaceAll, rfIgnoreCase]);
        TempStr := StringReplace(TempStr, '%ID%', FSourceInfo.UniqueInstanceID, [rfReplaceAll, rfIgnoreCase]);

        Result := TempStr;
      end;
  end;
end;

procedure TGlobalLogger.AddHandler(Handler: TLogHandler);
begin
  if Handler = nil then Exit;

  FLock.Enter;
  try
    if FHandlers.IndexOf(Handler) < 0 then
    begin
      FHandlers.Add(Handler);
      Handler.LogLevels := FLogLevels;  // 기본 로그 레벨 설정
      Handler.Init;  // 핸들러 초기화

      // 신규: DatabaseHandler 참조 저장
      if Handler is TDatabaseHandler then
        FDatabaseHandler := TDatabaseHandler(Handler);
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.RemoveHandler(Handler: TLogHandler);
var
  Index: Integer;
begin
  if Handler = nil then Exit;

  FLock.Enter;
  try
    Index := FHandlers.IndexOf(Handler);
    if Index >= 0 then
    begin
      Handler.Shutdown;  // 핸들러 종료
      FHandlers.Delete(Index);
      Handler.Free;  // 핸들러 해제

      // 특수 핸들러 레퍼런스 정리
      if Handler = FLogServerHandler then
        FLogServerHandler := nil
      else if Handler = FEmailHandler then
        FEmailHandler := nil
      else if Handler = FDatabaseHandler then
        FDatabaseHandler := nil;
    end;
  finally
    FLock.Leave;
  end;
end;

function TGlobalLogger.FindHandler(HandlerClass: TClass): TLogHandler;
var
  i: Integer;
  Handler: TLogHandler;
begin
  Result := nil;

  FLock.Enter;
  try
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      if Handler is HandlerClass then
      begin
        Result := Handler;
        Break;
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.ClearHandlers;
var
  i: Integer;
  Handler: TLogHandler;
begin
  FLock.Enter;
  try
    for i := FHandlers.Count - 1 downto 0 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      Handler.Shutdown;
      Handler.Free;
    end;
    FHandlers.Clear;

    // 추가 핸들러 레퍼런스 정리
    FLogServerHandler := nil;
    FEmailHandler := nil;
    FDatabaseHandler := nil;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.Log(const Msg: string; Level: TLogLevel; const Tag: string = '');
var
  i: Integer;
  Handler: TLogHandler;
  FormattedMsg: string;
  ExtDataID: Int64;
begin
  if not (Level in FLogLevels) then Exit;

  FLock.Enter;
  try
    // 신규: 확장 데이터 ID 가져오기
    ExtDataID := 0;
    if FUseExtendedData and (FExtDataMode <> edmNone) and (FDatabaseHandler <> nil) then
      ExtDataID := GetExtendedDataID(nil, Tag);

    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      if Handler.Active then
      begin
        // 각 핸들러별 메시지 포맷팅 (핸들러 정보 포함)
        FormattedMsg := FormatLogMessage(Level, Msg, Tag, Handler);

        // 신규: 확장 데이터 ID가 있는 경우 DatabaseHandler에 전달
        if (ExtDataID > 0) and (Handler is TDatabaseHandler) then
          TDatabaseHandler(Handler).DeliverWithExtData(FormattedMsg, Level, Tag, ExtDataID)
        else
          Handler.Deliver(FormattedMsg, Level, Tag);
      end;
    end;
  finally
    FLock.Leave;
  end;

  // 들여쓰기 자동 관리 - 함수 출입 관리
  if FIndentMode = imAuto then
  begin
    if Level = llDebug then
    begin
      // 함수 진입/종료 감지 - 메시지 내용으로 판단
      if (Pos('>>> 함수 진입:', Msg) > 0) or (Pos('entering', LowerCase(Msg)) > 0) then
        IncreaseIndent
      else if (Pos('<<< 함수 종료:', Msg) > 0) or (Pos('exiting', LowerCase(Msg)) > 0) then
        DecreaseIndent;
    end;
  end;
end;

procedure TGlobalLogger.LogFmt(const Fmt: string; const Args: array of const; Level: TLogLevel; const Tag: string);
begin
  Log(Format(Fmt, Args), Level, Tag);
end;

procedure TGlobalLogger.LogDevelop(const Msg: string; const Tag: string);
begin
  Log(Msg, llDevelop, Tag);
end;

procedure TGlobalLogger.LogDebug(const Msg: string; const Tag: string);
begin
  Log(Msg, llDebug, Tag);
end;

procedure TGlobalLogger.LogInfo(const Msg: string; const Tag: string);
begin
  Log(Msg, llInfo, Tag);
end;

procedure TGlobalLogger.LogWarning(const Msg: string; const Tag: string);
begin
  Log(Msg, llWarning, Tag);
end;

procedure TGlobalLogger.LogError(const Msg: string; const Tag: string);
begin
  Log(Msg, llError, Tag);
end;

procedure TGlobalLogger.LogFatal(const Msg: string; const Tag: string);
begin
  Log(Msg, llFatal, Tag);
end;

procedure TGlobalLogger.LogException(const Msg: string; E: Exception; Level: TLogLevel; const Tag: string);
begin
  Log(Format('%s: %s - %s', [Msg, E.ClassName, E.Message]), Level, Tag);
end;

procedure TGlobalLogger.Send(const Msg: string);
begin
  LogInfo(Msg);
end;

procedure TGlobalLogger.SendError(const Msg: string);
begin
  LogError(Msg);
end;

procedure TGlobalLogger.SendException(const Msg: string; E: Exception);
begin
  LogException(Msg, E);
end;


function TGlobalLogger.AddFileHandler(const LogPath, FilePrefix: string): TFileHandler;
begin
  Result := TFileHandler.Create(LogPath, FilePrefix);
  AddHandler(Result);
end;

function TGlobalLogger.AddMemoHandler(AMemo: TCustomMemo): TMemoHandler;
begin
  Result := TMemoHandler.Create(AMemo);
  AddHandler(Result);
end;

function TGlobalLogger.AddConsoleHandler: TConsoleHandler;
begin
  Result := TConsoleHandler.Create;
  AddHandler(Result);
end;

function TGlobalLogger.AddNetworkHandler(const Host: string; Port: Integer): TNetworkHandler;
begin
  Result := TNetworkHandler.Create(Host, Port);
  AddHandler(Result);
end;

// 시리얼 핸들러
function TGlobalLogger.AddSerialHandler(ASerialPort: TComPort; OwnsPort: Boolean): TSerialHandler;
var
  SerialHandler: TSerialHandler;
begin
  FLock.Enter;
  try
    // 새 시리얼 핸들러 생성
    SerialHandler := TSerialHandler.Create(ASerialPort, OwnsPort);

    // 핸들러 등록
    AddHandler(SerialHandler);

    Result := SerialHandler;
  finally
    FLock.Leave;
  end;
end;

// DatabaseHandler
function TGlobalLogger.AddDatabaseHandler(const AHost, ADatabase, AUser, APassword: string): TDatabaseHandler;
begin
  Result := TDatabaseHandler.Create(AHost, ADatabase, AUser, APassword);
  // 신규: 확장 데이터 사용 설정
  Result.UseExtendedData := FUseExtendedData;
  AddHandler(Result);
  FDatabaseHandler := Result;
end;


function TGlobalLogger.AddLogServerHandler(Port: Integer): TLogServerHandler;
var
  ServerHandler: TLogServerHandler;
begin
  FLock.Enter;
  try
    // 이미 존재하는 로그 서버 핸들러 확인
    if FLogServerHandler <> nil then
    begin
      Result := FLogServerHandler;
      Exit;
    end;

    // 새 로그 서버 핸들러 생성
    ServerHandler := TLogServerHandler.Create(Port);

    // 이벤트 연결
    if Assigned(FOnLogReceived) then
      ServerHandler.OnLogReceived := FOnLogReceived;

    // 핸들러 등록
    AddHandler(ServerHandler);

    // 참조 저장
    FLogServerHandler := ServerHandler;
    Result := ServerHandler;
  finally
    FLock.Leave;
  end;
end;

function TGlobalLogger.AddEmailHandler: TLogEmailHandler;
var
  EmailHandler: TLogEmailHandler;
begin
  FLock.Enter;
  try
    // 이미 존재하는 이메일 핸들러 확인
    if FEmailHandler <> nil then
    begin
      Result := FEmailHandler;
      Exit;
    end;

    // 새 이메일 핸들러 생성
    EmailHandler := TLogEmailHandler.Create;

    // 기본 설정
    EmailHandler.LogLevels := [llError, llFatal]; // 기본적으로 에러와 치명적 오류만

    // 핸들러 등록
    AddHandler(EmailHandler);

    // 참조 저장
    FEmailHandler := EmailHandler;
    Result := EmailHandler;
  finally
    FLock.Leave;
  end;
end;

function TGlobalLogger.WrapWithSourceIdentifier(Handler: TLogHandler; OwnsHandler: Boolean): TSourceIdentifierHandler;
var
  SourceHandler: TSourceIdentifierHandler;
begin
  if Handler = nil then
  begin
    Result := nil;
    Exit;
  end;

  FLock.Enter;
  try
    // 소스 식별자 핸들러로 래핑
    SourceHandler := TSourceIdentifierHandler.Create(Handler, OwnsHandler);

    // 소스 식별자 설정
    SourceHandler.Format := FSourceIdentifierFormat;
    if FSourceIdentifierFormat = sfCustom then
      SourceHandler.CustomFormat := FSourceCustomFormat;

    // 소스 정보 설정 (GlobalLogger에서 관리하는 정보로)
    SourceHandler.SourceInfo := FSourceInfo;

    // 핸들러 목록에서 원본 제거 (OwnsHandler=False인 경우)
    if OwnsHandler then
      RemoveHandler(Handler, False);

    // 래핑된 핸들러 추가
    AddHandler(SourceHandler);

    Result := SourceHandler;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.EnableSourceIdentifier(Format: TSourceIdentifierFormat; const CustomFormat: string);
begin
  FLock.Enter;
  try
    FUseSourceIdentifier := True;
    FSourceIdentifierFormat := Format;

    if CustomFormat <> '' then
    begin
      FSourceCustomFormat := CustomFormat;
      FSourceIdentifierFormat := sfCustom;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.SetSourceInfo(const HostName, IPAddress, AppName, AppVersion, UserName: string);
var
  Guid: TGUID;
begin
  FLock.Enter;
  try
    // 소스 정보 수동 설정
    FSourceInfo.HostName := HostName;
    FSourceInfo.IPAddress := IPAddress;
    FSourceInfo.ApplicationName := AppName;
    FSourceInfo.ApplicationVersion := AppVersion;
    FSourceInfo.UserName := UserName;

    // 프로세스 ID 설정 (OS별 처리)
    {$IFDEF MSWINDOWS}
    FSourceInfo.ProcessID := GetCurrentProcessId;

    // 고유 식별자 생성
    if CreateGUID(Guid) = S_OK then
      FSourceInfo.UniqueInstanceID := GUIDToString(Guid) + '-' + IntToStr(FSourceInfo.ProcessID);
    {$ENDIF}

    {$IFDEF UNIX}
    FSourceInfo.ProcessID := fpGetPID;

    // 고유 식별자 생성
    Randomize;
    FSourceInfo.UniqueInstanceID := Format('%s-%d-%d',
      [FormatDateTime('yyyymmddhhnnsszzz', Now),
       FSourceInfo.ProcessID,
       Random(1000000)]);
    {$ENDIF}
  finally
    FLock.Leave;
  end;
end;

function TGlobalLogger.GetSourceInfo: TSourceInfo;
begin
  FLock.Enter;
  try
    Result := FSourceInfo;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.ConfigureSourceFormat(DisplayMode: TSourceDisplayMode;
                                            const Prefix: string;
                                            const Suffix: string;
                                            const Separator: string;
                                            SourceFirst: Boolean);
begin
  FLock.Enter;
  try
    FSourceFormat.DisplayMode := DisplayMode;
    FSourceFormat.Prefix := Prefix;
    FSourceFormat.Suffix := Suffix;
    FSourceFormat.Separator := Separator;
    FSourceFormat.SourceFirst := SourceFirst;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.SetHandlerSourceId(Handler: TLogHandler; const SourceId: string);
begin
  if not Assigned(Handler) then
    Exit;

  FLock.Enter;
  try
    Handler.SourceIdentifier := SourceId;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.AutoConfigureHandlerSources;
var
  i: Integer;
  Handler: TLogHandler;
begin
  FLock.Enter;
  try
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);

      // 핸들러 타입별 소스 ID 자동 설정
      if Handler is TFileHandler then
        Handler.SourceIdentifier := 'FILE'
      else if Handler is TMemoHandler then
        Handler.SourceIdentifier := 'MEMO'
      else if Handler is TConsoleHandler then
        Handler.SourceIdentifier := 'CONSOLE'
      else if Handler is TNetworkHandler then
        Handler.SourceIdentifier := 'NETWORK'
      else if Handler is TPrintfHandler then
        Handler.SourceIdentifier := 'PRINTF'
      else if Handler is TLogServerHandler then
        Handler.SourceIdentifier := 'SERVER'
      else if Handler is TLogEmailHandler then
        Handler.SourceIdentifier := 'EMAIL'
      else if Handler is TSerialHandler then
        Handler.SourceIdentifier := 'SERIAL'
      else if Handler is TBufferedHandler then
        Handler.SourceIdentifier := 'BUFFER'
      else if Handler is TSourceIdentifierHandler then
        Handler.SourceIdentifier := 'SOURCE'
      else if Handler is TDatabaseHandler then
        Handler.SourceIdentifier := 'DATABASE'
      else
        Handler.SourceIdentifier := Handler.ClassName;
    end;
  finally
    FLock.Leave;
  end;
end;


procedure TGlobalLogger.StartLogServer(Port: Integer);
var
  Server: TLogServerHandler;
begin
  FLock.Enter;
  try
    // 기존 서버 확인
    Server := TLogServerHandler(FindHandler(TLogServerHandler));

    // 서버가 없으면 생성
    if Server = nil then
      Server := AddLogServerHandler(Port);

    // 서버 시작
    try
      if not Server.ServerStarted then
        Server.StartServer;
    except
      on E: Exception do
        LogError('로그 서버 시작 실패: ' + E.Message);
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.StopLogServer;
var
  Server: TLogServerHandler;
begin
  FLock.Enter;
  try
    // 서버 찾기
    Server := TLogServerHandler(FindHandler(TLogServerHandler));

    // 서버가 있으면 정지
    if (Server <> nil) and Server.ServerStarted then
    try
      Server.StopServer;
    except
      on E: Exception do
        LogError('로그 서버 정지 실패: ' + E.Message);
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.AddAllowedServerIP(const IPAddress: string);
var
  Server: TLogServerHandler;
begin
  if IPAddress = '' then Exit;

  FLock.Enter;
  try
    // 서버 찾기
    Server := TLogServerHandler(FindHandler(TLogServerHandler));

    // 서버가 있으면 IP 추가
    if Server <> nil then
      Server.AddAllowedIP(IPAddress);
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.ClearAllowedServerIPs;
var
  Server: TLogServerHandler;
begin
  FLock.Enter;
  try
    // 서버 찾기
    Server := TLogServerHandler(FindHandler(TLogServerHandler));

    // 서버가 있으면 IP 목록 초기화
    if Server <> nil then
      Server.ClearAllowedIPs;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.ConfigureEmailSettings(const SmtpServer: string; SmtpPort: Integer;
                                             const Username, Password: string; UseSSL: Boolean);
var
  Email: TLogEmailHandler;
begin
  FLock.Enter;
  try
    // 이메일 핸들러 찾기
    Email := TLogEmailHandler(FindHandler(TLogEmailHandler));

    // 이메일 핸들러가 없으면 생성
    if Email = nil then
      Email := AddEmailHandler;

    try
      // SMTP 설정
      Email.SmtpServer := SmtpServer;
      Email.SmtpPort := SmtpPort;
      Email.SmtpUser := Username;
      Email.SmtpPassword := Password;
      Email.UseSSL := UseSSL;
    except
      on E: Exception do
        LogError('이메일 설정 구성 실패: ' + E.Message);
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.SetEmailAddresses(const FromAddr, ToAddrs, CcAddrs: string);
var
  Email: TLogEmailHandler;
begin
  FLock.Enter;
  try
    // 이메일 핸들러 찾기
    Email := TLogEmailHandler(FindHandler(TLogEmailHandler));

    // 이메일 핸들러가 없으면 생성
    if Email = nil then
      Email := AddEmailHandler;

    try
      // 이메일 주소 설정
      if FromAddr <> '' then
        Email.FromAddress := FromAddr;

      if ToAddrs <> '' then
        Email.ToAddressesAsString := ToAddrs;

      if CcAddrs <> '' then
        Email.CcAddressesAsString := CcAddrs;
    except
      on E: Exception do
        LogError('이메일 주소 설정 실패: ' + E.Message);
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.SetEmailLogLevels(LogLevels: TLogLevelSet);
var
  Email: TLogEmailHandler;
begin
  FLock.Enter;
  try
    // 이메일 핸들러 찾기
    Email := TLogEmailHandler(FindHandler(TLogEmailHandler));

    // 이메일 핸들러가 없으면 생성
    if Email = nil then
      Email := AddEmailHandler;

    // 이메일 로그 레벨 설정
    Email.EmailLogLevels := LogLevels;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.Configure(const LogPath, FilePrefix: string;
                                AMemo: TCustomMemo;
                                ALogLevels: TLogLevelSet);
var
  FileHandler: TFileHandler;
  MemoHandler: TMemoHandler;
  i: Integer;
  Handler: TLogHandler;
begin
  FLock.Enter;
  try
    // 기존 핸들러 제거
    ClearHandlers;

    // 로그 레벨 설정
    FLogLevels := ALogLevels;

    // 파일 핸들러 추가
    try
      FileHandler := AddFileHandler(LogPath, FilePrefix);
      FileHandler.RotationMode := lrmDate; // 기본값
    except
      on E: Exception do
        WriteLn('파일 핸들러 생성 실패: ', E.Message);
    end;

    // 메모 핸들러가 요청된 경우 추가
    if Assigned(AMemo) then
    begin
      try
        MemoHandler := AddMemoHandler(AMemo);
      except
        on E: Exception do
          WriteLn('메모 핸들러 생성 실패: ', E.Message);
      end;
    end;

    // 모든 핸들러에 로그 레벨 설정
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      Handler.LogLevels := FLogLevels;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.StartLogSession;
var
  i: Integer;
  Handler: TLogHandler;
  FileHandler: TFileHandler;
  MemoHandler: TMemoHandler;
begin
  FLock.Enter;
  try
    // 각 핸들러에 세션 시작 알림
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);

      // 파일 핸들러인 경우
      if Handler is TFileHandler then
      begin
        FileHandler := TFileHandler(Handler);
        try
          FileHandler.WriteSessionStart;
        except
          on E: Exception do
            LogError('세션 시작 기록 실패(파일): ' + E.Message);
        end;
      end
      // 메모 핸들러인 경우
      else if Handler is TMemoHandler then
      begin
        MemoHandler := TMemoHandler(Handler);
        try
          MemoHandler.WriteSessionStart;
        except
          on E: Exception do
            LogError('세션 시작 기록 실패(메모): ' + E.Message);
        end;
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

// 버퍼링 핸들러
function TGlobalLogger.WrapWithBuffer(Handler: TLogHandler; BufferSize: Integer = 100): TBufferedHandler;
begin
  if Handler = nil then
  begin
    Result := nil;
    Exit;
  end;

  FLock.Enter;
  try
    // 기존 핸들러를 버퍼링으로 래핑
    Result := TBufferedHandler.Create(Handler, False);
    Result.BufferSize := BufferSize;

    // 기존 핸들러 제거하고 버퍼링 핸들러 추가
    RemoveHandler(Handler, False);  // 해제하지 않고 제거
    AddHandler(Result);
  finally
    FLock.Leave;
  end;
end;

// 확장된 RemoveHandler - 핸들러 해제 여부 지정 가능
procedure TGlobalLogger.RemoveHandler(Handler: TLogHandler; FreeHandler: Boolean = True);
var
  Index: Integer;
begin
  if Handler = nil then Exit;

  FLock.Enter;
  try
    Index := FHandlers.IndexOf(Handler);
    if Index >= 0 then
    begin
      Handler.Shutdown;  // 핸들러 종료
      FHandlers.Delete(Index);

      // 특수 핸들러 레퍼런스 정리
      if Handler = FLogServerHandler then
        FLogServerHandler := nil
      else if Handler = FEmailHandler then
        FEmailHandler := nil
      else if Handler = FPrintfHandler then
        FPrintfHandler := nil
      else if Handler = FDatabaseHandler then
        FDatabaseHandler := nil;

      if FreeHandler then
        Handler.Free;  // 핸들러 해제 (옵션)
    end;
  finally
    FLock.Leave;
  end;
end;

// 모든 버퍼 플러시
procedure TGlobalLogger.FlushAllBuffers;
var
  i: Integer;
  Handler: TLogHandler;
begin
  FLock.Enter;
  try
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);

      // 버퍼링 핸들러인 경우 플러시
      if Handler is TBufferedHandler then
      try
        TBufferedHandler(Handler).Flush;
      except
        on E: Exception do
          LogError('버퍼 플러시 실패: ' + E.Message);
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

// 버퍼링 활성화
procedure TGlobalLogger.EnableBuffering(HandlerClass: TClass = nil; BufferSize: Integer = 100);
var
  i: Integer;
  Handler: TLogHandler;
  HandlersToWrap: TList;
begin
  HandlersToWrap := TList.Create;
  try
    FLock.Enter;
    try
      // 래핑할 핸들러 목록 준비
      for i := 0 to FHandlers.Count - 1 do
      begin
        Handler := TLogHandler(FHandlers[i]);

        // 이미 버퍼링된 핸들러는 제외
        if Handler is TBufferedHandler then
          Continue;

        // 특정 클래스만 요청된 경우 확인
        if (HandlerClass <> nil) and not (Handler is HandlerClass) then
          Continue;

        HandlersToWrap.Add(Handler);
      end;

      // 래핑할 핸들러가 없으면 종료
      if HandlersToWrap.Count = 0 then
        Exit;

      // 각 핸들러를 버퍼링으로 래핑
      for i := 0 to HandlersToWrap.Count - 1 do
      begin
        Handler := TLogHandler(HandlersToWrap[i]);
        try
          WrapWithBuffer(Handler, BufferSize);
        except
          on E: Exception do
            LogError('버퍼링 활성화 실패: ' + E.Message);
        end;
      end;
    finally
      FLock.Leave;
    end;
  finally
    HandlersToWrap.Free;
  end;
end;

// 네트워크 로깅 활성화
procedure TGlobalLogger.EnableNetworkLogging(const Host: string; Port: Integer; Buffered: Boolean = True);
var
  NetworkHandler: TNetworkHandler;
begin
  if (Host = '') then
  begin
    LogError('네트워크 로깅 활성화 실패: 호스트가 지정되지 않음');
    Exit;
  end;

  FLock.Enter;
  try
    try
      // 네트워크 핸들러 추가
      NetworkHandler := AddNetworkHandler(Host, Port);

      // 버퍼링 요청 시 래핑
      if Buffered then
        WrapWithBuffer(NetworkHandler, 50);  // 네트워크는 작은 버퍼 사용
    except
      on E: Exception do
        LogError('네트워크 로깅 활성화 실패: ' + E.Message);
    end;
  finally
    FLock.Leave;
  end;
end;

// 직렬 포트 로깅 활성화
procedure TGlobalLogger.EnableSerialLogging(const PortName: string; BaudRate: TBaudRate;
                                          Buffered: Boolean);
var
  SerialHandler: TSerialHandler;
begin
  if (PortName = '') then
  begin
    LogError('시리얼 로깅 활성화 실패: 포트가 지정되지 않음');
    Exit;
  end;

  FLock.Enter;
  try
    try
      // 시리얼 핸들러 추가
      SerialHandler := AddSerialHandler(nil, True);

      // 시리얼 포트 설정
      SerialHandler.ConfigurePort(PortName, BaudRate);

      // 버퍼링 요청 시 래핑
      if Buffered then
        WrapWithBuffer(SerialHandler, 50);  // 시리얼은 작은 버퍼 사용
    except
      on E: Exception do
        LogError('시리얼 로깅 활성화 실패: ' + E.Message);
    end;
  finally
    FLock.Leave;
  end;
end;


// Printf 핸들러 추가
function TGlobalLogger.AddPrintfHandler: TPrintfHandler;
var
  PrintfHandler: TPrintfHandler;
begin
  FLock.Enter;
  try
    // 이미 존재하는 Printf 핸들러 확인
    if FPrintfHandler <> nil then
    begin
      Result := FPrintfHandler;
      Exit;
    end;

    // 새 Printf 핸들러 생성
    PrintfHandler := TPrintfHandler.Create;

    // 기본 설정
    PrintfHandler.LogLevels := FLogLevels;
    PrintfHandler.ShowTime := True;
    PrintfHandler.ColorByLevel := True;
    PrintfHandler.StayOnTop := False;

    // 핸들러 등록
    AddHandler(PrintfHandler);

    // 참조 저장
    FPrintfHandler := PrintfHandler;
    Result := PrintfHandler;
  finally
    FLock.Leave;
  end;
end;

// Printf 윈도우 활성화
procedure TGlobalLogger.EnablePrintfWindow(const Caption: string; Width, Height: Integer;
                                         Position: TFormPosition;
                                         AutoRotate: Boolean = False;
                                         RotateLines: Integer = 5000);
var
  PrintfHandler: TPrintfHandler;
begin
  FLock.Enter;
  try
    try
      // Printf 핸들러 추가
      PrintfHandler := AddPrintfHandler;

      // 폼 설정
      PrintfHandler.SetFormDesign(Caption, Width, Height, Position);

      // 자동 로테이션 설정
      PrintfHandler.AutoRotate := AutoRotate;
      PrintfHandler.RotateLineCount := RotateLines;
      PrintfHandler.RotateSaveBeforeClear := True;

      // 로그 폴더 설정
      PrintfHandler.RotateLogFolder := ExtractFilePath(Application.ExeName) + 'logs' + PathDelim + 'printf';

      // 폼 표시
      PrintfHandler.ShowForm(True);
    except
      on E: Exception do
        LogError('Printf 윈도우 활성화 실패: ' + E.Message);
    end;
  finally
    FLock.Leave;
  end;
end;

// ConfigurePrintfRotation 메소드 추가 - 기존 PrintfHandler 설정 업데이트
procedure TGlobalLogger.ConfigurePrintfRotation(AutoRotate: Boolean; RotateLines: Integer;
                                               SaveBeforeClear: Boolean = True;
                                               const LogFolder: string = '');
var
  PrintfHandler: TPrintfHandler;
  FolderPath: string;
begin
  FLock.Enter;
  try
    // 기존 Printf 핸들러 찾기
    PrintfHandler := TPrintfHandler(FindHandler(TPrintfHandler));

    // 없으면 생성
    if PrintfHandler = nil then
      PrintfHandler := AddPrintfHandler;

    // 로테이션 설정
    PrintfHandler.AutoRotate := AutoRotate;
    PrintfHandler.RotateLineCount := RotateLines;
    PrintfHandler.RotateSaveBeforeClear := SaveBeforeClear;

    // 로그 폴더 설정
    if LogFolder <> '' then
      FolderPath := LogFolder
    else
      FolderPath := ExtractFilePath(Application.ExeName) + 'logs' + PathDelim + 'printf';

    PrintfHandler.RotateLogFolder := FolderPath;

    // 폴더 생성
    if not DirectoryExists(FolderPath) then
      ForceDirectories(FolderPath);
  finally
    FLock.Leave;
  end;
end;

function TGlobalLogger.GetIndentString: string;
var
  i: Integer;
begin
  if (FIndentMode = imNone) or (FIndentLevel <= 0) then
    Result := ''
  else
  begin
    SetLength(Result, FIndentLevel * FIndentSize);
    for i := 1 to Length(Result) do
      Result[i] := FIndentChar;
  end;
end;

procedure TGlobalLogger.IncreaseIndent;
begin
  FLock.Enter;
  try
    Inc(FIndentLevel);
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.DecreaseIndent;
begin
  FLock.Enter;
  try
    if FIndentLevel > 0 then
      Dec(FIndentLevel);
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.ResetIndent;
begin
  FLock.Enter;
  try
    FIndentLevel := 0;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.LogWithIndent(const Msg: string; Level: TLogLevel; const Tag: string);
begin
  // 인덴트가 이미 적용된 메시지 포맷 사용
  Log(Msg, Level, Tag);
end;

procedure TGlobalLogger.LogFunctionEnter(const FuncName: string);
begin
  Log('>>> 함수 진입: ' + FuncName, llDebug);
  // IncreaseIndent는 자동으로 처리됨 (imAuto 모드인 경우)
end;

procedure TGlobalLogger.LogFunctionExit(const FuncName: string);
begin
  // DecreaseIndent는 자동으로 처리됨 (imAuto 모드인 경우)
  Log('<<< 함수 종료: ' + FuncName, llDebug);
end;

procedure TGlobalLogger.StartTiming(const TimingLabel: string);
var
  Timing: PTimingInfo;
  Index: Integer;
begin
  if TimingLabel = '' then Exit;

  FLock.Enter;
  try
    // 이미 존재하는 라벨이면 업데이트
    Index := FTimings.IndexOf(TimingLabel);
    if Index >= 0 then
    begin
      Timing := PTimingInfo(FTimings.Objects[Index]);
      Timing^.StartTime := Now;
    end
    else
    begin
      try
        // 새 타이밍 정보 생성
        New(Timing);
        Timing^.TimingLabel := TimingLabel;
        Timing^.StartTime := Now;

        FTimings.AddObject(TimingLabel, TObject(Timing));
      except
        on E: Exception do
        begin
          LogError('타이밍 시작 실패: ' + E.Message);
          if Assigned(Timing) then
            Dispose(Timing);
        end;
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.EndTiming(const TimingLabel: string; Level: TLogLevel);
var
  Timing: PTimingInfo;
  Index: Integer;
  ElapsedMS: Int64;
begin
  if TimingLabel = '' then Exit;

  ElapsedMS := GetElapsedTime(TimingLabel);

  if ElapsedMS >= 0 then
  begin
    // 타이밍 로깅
    Log(Format('타이밍 [%s]: %d ms', [TimingLabel, ElapsedMS]), Level);

    // 타이밍 정보 제거
    FLock.Enter;
    try
      Index := FTimings.IndexOf(TimingLabel);
      if Index >= 0 then
      begin
        Timing := PTimingInfo(FTimings.Objects[Index]);
        Dispose(Timing);
        FTimings.Delete(Index);
      end;
    finally
      FLock.Leave;
    end;
  end
  else
    // 타이밍 정보를 찾을 수 없음
    Log(Format('타이밍 [%s]: 시작 정보를 찾을 수 없음', [TimingLabel]), llWarning);
end;

function TGlobalLogger.GetElapsedTime(const TimingLabel: string): Int64;
var
  Timing: PTimingInfo;
  Index: Integer;
begin
  Result := -1;  // 기본값: 타이밍 정보 없음

  if TimingLabel = '' then Exit;

  FLock.Enter;
  try
    Index := FTimings.IndexOf(TimingLabel);
    if Index >= 0 then
    begin
      Timing := PTimingInfo(FTimings.Objects[Index]);
      Result := MilliSecondsBetween(Now, Timing^.StartTime);
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.ClearTimings;
var
  i: Integer;
  Timing: PTimingInfo;
begin
  FLock.Enter;
  try
    for i := 0 to FTimings.Count - 1 do
    begin
      Timing := PTimingInfo(FTimings.Objects[i]);
      Dispose(Timing);
    end;
    FTimings.Clear;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.EnableFilter(const FilterText: string; CaseSensitive: Boolean);
var
  i: Integer;
  Handler: TLogHandler;
begin
  if FilterText = '' then Exit;

  FLock.Enter;
  try
    // 모든 핸들러에 필터 설정
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      try
        Handler.EnableFilter(FilterText, CaseSensitive);
      except
        on E: Exception do
          LogError('필터 활성화 실패: ' + E.Message);
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.DisableFilter;
var
  i: Integer;
  Handler: TLogHandler;
begin
  FLock.Enter;
  try
    // 모든 핸들러에 필터 해제
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      try
        Handler.DisableFilter;
      except
        on E: Exception do
          LogError('필터 비활성화 실패: ' + E.Message);
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.LogWithTag(const Tag, Msg: string; Level: TLogLevel);
begin
  Log(Msg, Level, Tag);
end;

procedure TGlobalLogger.LogFmtWithTag(const Tag, Fmt: string; const Args: array of const; Level: TLogLevel);
begin
  LogWithTag(Tag, Format(Fmt, Args), Level);
end;

procedure TGlobalLogger.EnableTagFilter(const Tags: array of string);
var
  i, j: Integer;
  Handler: TLogHandler;
begin
  FLock.Enter;
  try
    // 모든 핸들러에 태그 필터 설정
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      try
        Handler.ClearTagFilters;
        for j := Low(Tags) to High(Tags) do
          Handler.AddTagFilter(Tags[j]);
      except
        on E: Exception do
          LogError('태그 필터 활성화 실패: ' + E.Message);
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.DisableTagFilter;
var
  i: Integer;
  Handler: TLogHandler;
begin
  FLock.Enter;
  try
    // 모든 핸들러에 태그 필터 해제
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      try
        Handler.ClearTagFilters;
      except
        on E: Exception do
          LogError('태그 필터 비활성화 실패: ' + E.Message);
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TGlobalLogger.EnableAsyncLogging(Enable: Boolean);
var
  i: Integer;
  Handler: TLogHandler;
begin
  FLock.Enter;
  try
    // 모든 핸들러에 비동기 모드 설정
    for i := 0 to FHandlers.Count - 1 do
    begin
      Handler := TLogHandler(FHandlers[i]);
      try
        if Enable then
          Handler.SetAsyncMode(amThread)
        else
          Handler.SetAsyncMode(amNone);
      except
        on E: Exception do
          LogError('비동기 로깅 설정 실패: ' + E.Message);
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

// 신규: 확장 데이터 모드 설정
procedure TGlobalLogger.SetExtendedDataMode(Mode: TExtDataMode);
begin
  FLock.Enter;
  try
    FExtDataMode := Mode;

    // 데이터베이스 핸들러가 있으면 확장 데이터 사용 설정
    if FDatabaseHandler <> nil then
      FDatabaseHandler.UseExtendedData := (Mode <> edmNone) and FUseExtendedData;
  finally
    FLock.Leave;
  end;
end;

// 신규: 데이터베이스에 확장 데이터 저장 활성화
procedure TGlobalLogger.EnableExtendedDataStorage(Enable: Boolean);
begin
  FLock.Enter;
  try
    FUseExtendedData := Enable;

    // 데이터베이스 핸들러가 있으면 확장 데이터 사용 설정
    if FDatabaseHandler <> nil then
      FDatabaseHandler.UseExtendedData := Enable and (FExtDataMode <> edmNone);
  finally
    FLock.Leave;
  end;
end;

// 신규: 사용자 정의 확장 데이터 필드 추가
procedure TGlobalLogger.AddCustomExtDataField(const FieldName: string);
begin
  if FieldName = '' then Exit;

  FLock.Enter;
  try
    if FCustomExtDataFields.IndexOf(FieldName) < 0 then
      FCustomExtDataFields.Add(FieldName);
  finally
    FLock.Leave;
  end;
end;

// 신규: 사용자 정의 확장 데이터 필드 제거
procedure TGlobalLogger.RemoveCustomExtDataField(const FieldName: string);
var
  Index: Integer;
begin
  if FieldName = '' then Exit;

  FLock.Enter;
  try
    Index := FCustomExtDataFields.IndexOf(FieldName);
    if Index >= 0 then
      FCustomExtDataFields.Delete(Index);
  finally
    FLock.Leave;
  end;
end;

// 신규: 사용자 정의 확장 데이터 필드 지우기
procedure TGlobalLogger.ClearCustomExtDataFields;
begin
  FLock.Enter;
  try
    FCustomExtDataFields.Clear;
  finally
    FLock.Leave;
  end;
end;

// 신규: 확장 데이터 포함 로그 조회
function TGlobalLogger.GetLogsWithExtData(const StartDate, EndDate: TDateTime;
                                         const LogLevel: string = '';
                                         const SearchText: string = ''): TZQuery;
begin
  Result := nil;

  // 데이터베이스 핸들러가 없으면 찾기
  if FDatabaseHandler = nil then
    FDatabaseHandler := TDatabaseHandler(FindHandler(TDatabaseHandler));

  // 데이터베이스 핸들러가 없으면 종료
  if FDatabaseHandler = nil then
    Exit;

  // 확장 데이터 포함 로그 조회
  Result := FDatabaseHandler.GetLogWithExtData(StartDate, EndDate, LogLevel, SearchText);
end;

// 신규: JSON 필드 필터링으로 로그 조회
function TGlobalLogger.GetLogsByJsonFilter(const JsonPath, JsonValue: string): TZQuery;
var
  SQL: string;
begin
  Result := nil;

  // 데이터베이스 핸들러가 없으면 찾기
  if FDatabaseHandler = nil then
    FDatabaseHandler := TDatabaseHandler(FindHandler(TDatabaseHandler));

  // 데이터베이스 핸들러가 없으면 종료
  if FDatabaseHandler = nil then
    Exit;

  // JSON 필터링 쿼리 생성
  SQL := 'SELECT l.LOG_ID, l.TIMESTAMP, l.LEVEL, l.MESSAGE, l.TAG, l.SOURCE_ID, ' +
         'e.HOST_NAME, e.IP_ADDRESS, e.APP_NAME, e.USER_NAME, e.EXTRA_DATA ' +
         'FROM ' + FDatabaseHandler.CurrentMonthTable + ' l ' +
         'JOIN ' + FDatabaseHandler.ExtDataTableName + ' e ON l.EXT_DATA_ID = e.EXT_DATA_ID ' +
         'WHERE JSON_VALUE(e.EXTRA_DATA, :JSON_PATH) = :JSON_VALUE ' +
         'ORDER BY l.TIMESTAMP DESC';

  // 쿼리 실행
  Result := TZQuery.Create(nil);
  Result.Connection := FDatabaseHandler.Connection;
  Result.SQL.Text := SQL;
  Result.ParamByName('JSON_PATH').AsString := JsonPath;
  Result.ParamByName('JSON_VALUE').AsString := JsonValue;
  Result.Open;
end;

initialization
  TGlobalLogger.FInstance := nil;

finalization
  TGlobalLogger.FreeInstance;

end.
