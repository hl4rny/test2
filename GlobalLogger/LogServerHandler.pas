unit LogServerHandler;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, LogHandlers, SyncObjs, blcksock, DateUtils;

type
  // 로그 소스 정보
  TLogSourceInfo = record
    IPAddress: string;      // 소스 IP 주소
    HostName: string;       // 호스트명
    ApplicationID: string;  // 애플리케이션 ID
    UserID: string;         // 사용자 ID
  end;
  
  // 수신된 로그 정보
  TReceivedLogInfo = record
    Message: string;        // 원본 로그 메시지
    Level: TLogLevel;       // 로그 레벨
    Source: TLogSourceInfo; // 소스 정보
    Timestamp: TDateTime;   // 수신 시간
  end;
  
  // 로그 수신 이벤트 타입
  TLogReceiveEvent = procedure(const LogInfo: TReceivedLogInfo) of object;
  
  // 로그 서버 스레드 전방 선언
  TLogServerThread = class;
  TLogClientHandlerThread = class;
  
  { TLogServerHandler - 로그 수신 서버 핸들러 }
  TLogServerHandler = class(TLogHandler)
  private
    FLock: TCriticalSection;        // 스레드 동기화 객체
    FPort: Integer;                 // 서버 리스닝 포트
    FMaxConnections: Integer;       // 최대 동시 연결 수
    FServerThread: TLogServerThread; // 서버 리스닝 스레드
    FClientThreads: TThreadList;     // 클라이언트 처리 스레드 목록
    FOnLogReceived: TLogReceiveEvent; // 로그 수신 이벤트
    FServerStarted: Boolean;        // 서버 시작 여부
    FLogToLocalToo: Boolean;        // 수신된 로그를 로컬에도 기록할지 여부
    FCaptureSourceInfo: Boolean;    // 소스 정보 캡처 여부
    FAllowedIPs: TStringList;       // 허용 IP 목록 (비어있으면 모두 허용)
    
    // 클라이언트 연결 핸들링
    procedure HandleClientConnection(ClientSocket: TTCPBlockSocket);
    
    // 클라이언트 스레드 추가/제거
    procedure AddClientThread(Thread: TLogClientHandlerThread);
    procedure RemoveClientThread(Thread: TLogClientHandlerThread);
    
    // 로그 메시지 파싱
    function ParseLogMessage(const RawMessage: string; var LogInfo: TReceivedLogInfo): Boolean;
    
    // IP 허용 여부 확인
    function IsIPAllowed(const IPAddress: string): Boolean;
    
  protected
    // 핸들러 기본 메서드 (여기서는 사용하지 않음)
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;
    
  public
    constructor Create(Port: Integer = 9000); reintroduce;
    destructor Destroy; override;
    
    procedure Init; override;
    procedure Shutdown; override;
    
    // 서버 제어
    procedure StartServer;
    procedure StopServer;
    
    // IP 허용 목록 관리
    procedure AddAllowedIP(const IPAddress: string);
    procedure ClearAllowedIPs;
    
    // 속성
    property Port: Integer read FPort write FPort;
    property MaxConnections: Integer read FMaxConnections write FMaxConnections;
    property OnLogReceived: TLogReceiveEvent read FOnLogReceived write FOnLogReceived;
    property LogToLocalToo: Boolean read FLogToLocalToo write FLogToLocalToo;
    property CaptureSourceInfo: Boolean read FCaptureSourceInfo write FCaptureSourceInfo;
    property ServerStarted: Boolean read FServerStarted;
  end;
  
  { TLogServerThread - 서버 리스닝 스레드 }
  TLogServerThread = class(TThread)
  private
    FOwner: TLogServerHandler;
    FServerSocket: TTCPBlockSocket;
    FPort: Integer;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TLogServerHandler; APort: Integer);
    destructor Destroy; override;
  end;
  
  { TLogClientHandlerThread - 클라이언트 연결 처리 스레드 }
  TLogClientHandlerThread = class(TThread)
  private
    FOwner: TLogServerHandler;
    FClientSocket: TTCPBlockSocket;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TLogServerHandler; AClientSocket: TTCPBlockSocket);
    destructor Destroy; override;
  end;

implementation

{ TLogServerThread }

constructor TLogServerThread.Create(AOwner: TLogServerHandler; APort: Integer);
begin
  inherited Create(True);  // 일시 중단 상태로 생성
  
  FOwner := AOwner;
  FPort := APort;
  FServerSocket := TTCPBlockSocket.Create;
  
  FreeOnTerminate := False;
end;

destructor TLogServerThread.Destroy;
begin
  // 소켓 정리
  if Assigned(FServerSocket) then
  begin
    FServerSocket.CloseSocket;
    FServerSocket.Free;
  end;
  
  inherited;
end;

procedure TLogServerThread.Execute;
var
  ClientSocket: TTCPBlockSocket;
begin
  // 서버 소켓 초기화
  FServerSocket.CreateSocket;
  FServerSocket.SetLinger(True, 10);
  FServerSocket.Bind('0.0.0.0', IntToStr(FPort));
  FServerSocket.Listen;
  
  // 연결 대기 루프
  while not Terminated do
  begin
    // 새 연결 수락
    if FServerSocket.CanRead(1000) then
    begin
      // 클라이언트 소켓 생성
      ClientSocket := TTCPBlockSocket.Create;
      ClientSocket.Socket := FServerSocket.Accept;
      
      if ClientSocket.LastError = 0 then
      begin
        // 클라이언트 처리를 소유자에게 위임
        FOwner.HandleClientConnection(ClientSocket);
      end
      else
      begin
        // 연결 실패
        ClientSocket.Free;
      end;
    end;
    
    // 종료 요청 확인
    if Terminated then
      Break;
  end;
end;

{ TLogClientHandlerThread }

constructor TLogClientHandlerThread.Create(AOwner: TLogServerHandler; AClientSocket: TTCPBlockSocket);
begin
  inherited Create(True);  // 일시 중단 상태로 생성
  
  FOwner := AOwner;
  FClientSocket := AClientSocket;
  
  FreeOnTerminate := True;  // 자동 해제
  
  // 스레드 목록에 추가
  FOwner.AddClientThread(Self);
  
  // 스레드 시작
  Start;
end;

destructor TLogClientHandlerThread.Destroy;
begin
  // 스레드 목록에서 제거
  if Assigned(FOwner) then
    FOwner.RemoveClientThread(Self);
  
  // 소켓 정리
  if Assigned(FClientSocket) then
  begin
    FClientSocket.CloseSocket;
    FClientSocket.Free;
  end;
  
  inherited;
end;

procedure TLogClientHandlerThread.Execute;
var
  Line: string;
  LogInfo: TReceivedLogInfo;
begin
  if not Assigned(FClientSocket) then
    Exit;
  
  // 클라이언트 IP 확인
  if not FOwner.IsIPAllowed(FClientSocket.GetRemoteSinIP) then
  begin
    // 연결 차단
    FClientSocket.CloseSocket;
    Exit;
  end;
  
  try
    // 클라이언트로부터 데이터 읽기
    while not Terminated and (FClientSocket.LastError = 0) do
    begin
      // 한 줄 읽기 (타임아웃: 1초)
      Line := FClientSocket.RecvString(1000);
      
      if FClientSocket.LastError = 0 then
      begin
        if Line <> '' then
        begin
          // 로그 메시지 파싱
          if FOwner.ParseLogMessage(Line, LogInfo) then
          begin
            // 소스 IP 정보 설정
            LogInfo.Source.IPAddress := FClientSocket.GetRemoteSinIP;
            
            // 로그 수신 이벤트 호출
            if Assigned(FOwner.OnLogReceived) then
              FOwner.OnLogReceived(LogInfo);
              
            // 로컬에도 기록 (설정에 따라)
            if FOwner.LogToLocalToo then
            begin
              // 여기서는 GlobalLogger를 직접 사용하지 않고
              // 로그 핸들러 인터페이스를 통해 처리
              // (순환 참조 방지)
            end;
          end;
        end;
      end
      else
      begin
        // 연결 끊김 또는 오류
        Break;
      end;
    end;
  finally
    // 연결 종료
    FClientSocket.CloseSocket;
  end;
end;

{ TLogServerHandler }

constructor TLogServerHandler.Create(Port: Integer);
begin
  inherited Create;
  
  FPort := Port;
  FMaxConnections := 10;  // 기본값
  FServerThread := nil;
  FClientThreads := TThreadList.Create;
  FServerStarted := False;
  FLogToLocalToo := False;
  FCaptureSourceInfo := True;
  FAllowedIPs := TStringList.Create;
  FAllowedIPs.Sorted := True;
  FAllowedIPs.Duplicates := dupIgnore;
end;

destructor TLogServerHandler.Destroy;
begin
  // 서버 정지
  StopServer;
  
  // 클라이언트 스레드 목록 정리
  FClientThreads.Free;
  
  // IP 목록 정리
  FAllowedIPs.Free;
  
  inherited;
end;

procedure TLogServerHandler.Init;
begin
  inherited;
  
  // 기본 초기화
end;

procedure TLogServerHandler.Shutdown;
begin
  // 서버 정지
  StopServer;
  
  inherited;
end;

procedure TLogServerHandler.StartServer;
begin
  if FServerStarted then
    Exit;
  
  // 서버 스레드 생성 및 시작
  FServerThread := TLogServerThread.Create(Self, FPort);
  FServerThread.Start;
  FServerStarted := True;
end;

procedure TLogServerHandler.StopServer;
var
  ClientList: TList;
  i: Integer;
begin
  if not FServerStarted then
    Exit;
  
  // 서버 스레드 정지
  if Assigned(FServerThread) then
  begin
    FServerThread.Terminate;
    FServerThread.WaitFor;
    FServerThread.Free;
    FServerThread := nil;
  end;
  
  // 모든 클라이언트 스레드 정지
  ClientList := FClientThreads.LockList;
  try
    for i := ClientList.Count - 1 downto 0 do
    begin
      TLogClientHandlerThread(ClientList[i]).Terminate;
    end;
  finally
    FClientThreads.UnlockList;
  end;
  
  FServerStarted := False;
end;

procedure TLogServerHandler.HandleClientConnection(ClientSocket: TTCPBlockSocket);
var
  ClientThread: TLogClientHandlerThread;
  ClientList: TList;
begin
  // 최대 연결 수 확인
  ClientList := FClientThreads.LockList;
  try
    if ClientList.Count >= FMaxConnections then
    begin
      // 최대 연결 수 초과
      ClientSocket.Free;
      Exit;
    end;
  finally
    FClientThreads.UnlockList;
  end;
  
  // 클라이언트 처리 스레드 생성
  ClientThread := TLogClientHandlerThread.Create(Self, ClientSocket);
end;

procedure TLogServerHandler.AddClientThread(Thread: TLogClientHandlerThread);
begin
  if Assigned(Thread) then
    FClientThreads.Add(Thread);
end;

procedure TLogServerHandler.RemoveClientThread(Thread: TLogClientHandlerThread);
begin
  if Assigned(Thread) then
    FClientThreads.Remove(Thread);
end;

function TLogServerHandler.ParseLogMessage(const RawMessage: string; var LogInfo: TReceivedLogInfo): Boolean;
var
  LevelStr, DateTimeStr, RestMsg: string;
  OpenBracket1, CloseBracket1, OpenBracket2, CloseBracket2: Integer;
begin
  Result := False;
  
  // 기본값 초기화
  LogInfo.Message := RawMessage;
  LogInfo.Level := llInfo;
  LogInfo.Timestamp := Now;
  LogInfo.Source.IPAddress := '';
  LogInfo.Source.HostName := '';
  LogInfo.Source.ApplicationID := '';
  LogInfo.Source.UserID := '';
  
  // 표준 로그 형식 파싱: "[2023-04-15 12:34:56.789] [INFO] 메시지"
  OpenBracket1 := Pos('[', RawMessage);
  CloseBracket1 := Pos(']', RawMessage);
  
  if (OpenBracket1 > 0) and (CloseBracket1 > OpenBracket1) then
  begin
    // 날짜/시간 추출
    DateTimeStr := Copy(RawMessage, OpenBracket1 + 1, CloseBracket1 - OpenBracket1 - 1);
    
    // 레벨 추출
    OpenBracket2 := Pos('[', RawMessage, CloseBracket1 + 1);
    CloseBracket2 := Pos(']', RawMessage, OpenBracket2 + 1);
    
    if (OpenBracket2 > 0) and (CloseBracket2 > OpenBracket2) then
    begin
      LevelStr := Trim(Copy(RawMessage, OpenBracket2 + 1, CloseBracket2 - OpenBracket2 - 1));
      RestMsg := Trim(Copy(RawMessage, CloseBracket2 + 1, Length(RawMessage)));
      
      // 로그 레벨 파싱
      if LevelStr = 'DEVELOP' then
        LogInfo.Level := llDevelop
      else if LevelStr = 'DEBUG' then
        LogInfo.Level := llDebug
      else if LevelStr = 'INFO' then
        LogInfo.Level := llInfo
      else if LevelStr = 'WARNING' then
        LogInfo.Level := llWarning
      else if LevelStr = 'ERROR' then
        LogInfo.Level := llError
      else if LevelStr = 'FATAL' then
        LogInfo.Level := llFatal;
        
      // 소스 정보 파싱 (태그 포함 메시지: "[TAG] 실제 메시지")
      if FCaptureSourceInfo and (Length(RestMsg) > 0) and (RestMsg[1] = '[') then
      begin
        OpenBracket1 := 1;
        CloseBracket1 := Pos(']', RestMsg);
        
        if (CloseBracket1 > OpenBracket1) then
        begin
          // 태그를 애플리케이션 ID로 사용
          LogInfo.Source.ApplicationID := Trim(Copy(RestMsg, OpenBracket1 + 1, CloseBracket1 - OpenBracket1 - 1));
        end;
      end;
      
      Result := True;
    end;
  end;
end;

procedure TLogServerHandler.WriteLog(const Msg: string; Level: TLogLevel);
begin
  // 이 핸들러는 로그 작성이 아닌 수신을 담당하므로 사용하지 않음
  // 필요한 경우 여기에 구현
end;

procedure TLogServerHandler.AddAllowedIP(const IPAddress: string);
begin
  if IPAddress = '' then
    Exit;
    
  FLock.Enter;
  try
    FAllowedIPs.Add(IPAddress);
  finally
    FLock.Leave;
  end;
end;

procedure TLogServerHandler.ClearAllowedIPs;
begin
  FLock.Enter;
  try
    FAllowedIPs.Clear;
  finally
    FLock.Leave;
  end;
end;

function TLogServerHandler.IsIPAllowed(const IPAddress: string): Boolean;
begin
  FLock.Enter;
  try
    // 목록이 비어있으면 모든 IP 허용
    Result := (FAllowedIPs.Count = 0) or (FAllowedIPs.IndexOf(IPAddress) >= 0);
  finally
    FLock.Leave;
  end;
end;

end.
