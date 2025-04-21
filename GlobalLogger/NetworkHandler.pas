unit NetworkHandler;

{$mode objfpc}{$H+}

interface

uses
  Windows, Classes, SysUtils, LogHandlers, SyncObjs, blcksock, DateUtils;

type
  // 네트워크 로그 큐 아이템
  TNetLogQueueItem = record
    Message: string;
    Level: TLogLevel;
  end;
  PNetLogQueueItem = ^TNetLogQueueItem;
  
  { TNetworkHandlerThread - 네트워크 로깅용 스레드 }
  TNetworkHandlerThread = class(TThread)
  private
    FOwner: TObject;          // 소유자 (TNetworkHandler)
    FQueueEvent: TEvent;      // 큐 이벤트
    FTerminateEvent: TEvent;  // 종료 이벤트
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TObject);
    destructor Destroy; override;
    
    procedure SignalQueue;    // 큐 처리 신호
    procedure SignalTerminate; // 종료 신호
  end;
  
  { TNetworkHandler - 네트워크 로그 핸들러 }
  TNetworkHandler = class(TLogHandler)
  private
    FHost: string;              // 로그 서버 호스트명
    FPort: Integer;             // 로그 서버 포트
    FSocket: TTCPBlockSocket;   // 소켓 객체
    FReconnectInterval: Integer; // 재연결 간격(ms)
    FLastConnectAttempt: TDateTime; // 마지막 연결 시도 시간
    FLogQueue: TThreadList;      // 로그 메시지 큐
    FAsyncThread: TNetworkHandlerThread; // 비동기 처리 스레드
    FQueueMaxSize: Integer;      // 큐 최대 크기
    FQueueFlushInterval: Integer; // 큐 자동 플러시 간격
    FLastQueueFlush: TDateTime;  // 마지막 큐 플러시 시간
    
    function TryConnect: Boolean;    // 연결 시도
    procedure FlushQueue;           // 큐에 있는 로그 처리
    procedure ProcessLogQueue;      // 로그 큐 처리 (비동기 스레드에서 호출)
    function SendToNetwork(const Msg: string): Boolean; // 네트워크로 전송
    
  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;
    
  public
    constructor Create(const Host: string; Port: Integer); reintroduce;
    destructor Destroy; override;
    
    procedure Init; override;
    procedure Shutdown; override;
    
    // 세션 시작 마커 기록
    procedure WriteSessionStart;
    
    // 비동기 모드 설정 오버라이드
    procedure SetAsyncMode(Mode: TAsyncMode); override;
    
    // 속성
    property Host: string read FHost write FHost;
    property Port: Integer read FPort write FPort;
    property ReconnectInterval: Integer read FReconnectInterval write FReconnectInterval;
    property QueueMaxSize: Integer read FQueueMaxSize write FQueueMaxSize;
    property QueueFlushInterval: Integer read FQueueFlushInterval write FQueueFlushInterval;
  end;

implementation

{ TNetworkHandlerThread }

constructor TNetworkHandlerThread.Create(AOwner: TObject);
begin
  inherited Create(True); // 일시 중단 상태로 생성
  
  FOwner := AOwner;
  FQueueEvent := TEvent.Create(nil, False, False, '');
  FTerminateEvent := TEvent.Create(nil, True, False, '');
  
  FreeOnTerminate := False;
  
  // 스레드 시작
  Start;
end;

destructor TNetworkHandlerThread.Destroy;
begin
  FQueueEvent.Free;
  FTerminateEvent.Free;
  
  inherited;
end;



procedure TNetworkHandlerThread.Execute;
var
  WaitResult: DWORD;
  Events: array[0..1] of THandle;
begin
  Events[0] := THandle(FQueueEvent.Handle);  // 큐 이벤트 핸들
  Events[1] := THandle(FTerminateEvent.Handle);  // 종료 이벤트 핸들

  while not Terminated do
  begin
    // 이벤트 대기 (5초 타임아웃)
    WaitResult := WaitForMultipleObjects(2, @Events[0], False, 5000);

    if Terminated then
      Break;

    case WaitResult of
      WAIT_OBJECT_0:     // FQueueEvent 신호 (인덱스 0)
        TNetworkHandler(FOwner).ProcessLogQueue;
      WAIT_OBJECT_0 + 1: // FTerminateEvent 신호 (인덱스 1)
        Break;
      WAIT_TIMEOUT:      // 타임아웃 (5초 경과)
        TNetworkHandler(FOwner).ProcessLogQueue;
      else               // 오류 발생 (WAIT_FAILED 등)
        Break;
    end;
  end;
end;

procedure TNetworkHandlerThread.SignalQueue;
begin
  FQueueEvent.SetEvent;
end;

procedure TNetworkHandlerThread.SignalTerminate;
begin
  FTerminateEvent.SetEvent;
end;

{ TNetworkHandler }

constructor TNetworkHandler.Create(const Host: string; Port: Integer);
begin
  inherited Create;
  
  // 네트워크 설정
  FHost := Host;
  FPort := Port;
  FReconnectInterval := 30000; // 기본값: 30초
  FLastConnectAttempt := 0;
  FSocket := nil;
  
  // 비동기 처리 관련
  FLogQueue := TThreadList.Create;
  FAsyncThread := nil;
  FQueueMaxSize := 1000;       // 기본 큐 크기
  FQueueFlushInterval := 5000; // 기본 플러시 간격 (ms)
  FLastQueueFlush := Now;
end;

destructor TNetworkHandler.Destroy;
begin
  Shutdown;
  
  // 로그 큐 정리
  FlushQueue;
  FLogQueue.Free;
  
  // 소켓 정리
  if Assigned(FSocket) then
    FSocket.Free;
  
  inherited;
end;

procedure TNetworkHandler.Init;
begin
  inherited;
  
  // 소켓 생성
  if not Assigned(FSocket) then
    FSocket := TTCPBlockSocket.Create;
  
  // 연결 시도
  TryConnect;
end;

procedure TNetworkHandler.Shutdown;
begin
  // 비동기 스레드 종료
  SetAsyncMode(amNone);
  
  // 로그 큐 플러시
  FlushQueue;
  
  // 소켓 연결 종료
  if Assigned(FSocket) then
  begin
    FSocket.CloseSocket;
  end;
  
  inherited;
end;

function TNetworkHandler.TryConnect: Boolean;
begin
  Result := False;
  
  if not Assigned(FSocket) then
    Exit;
  
  // 이미 연결되어 있거나 재연결 시간이 지나지 않았으면 작업 안함
  if (FSocket.LastError = 0) or 
     ((Now - FLastConnectAttempt) < (FReconnectInterval / 86400000)) then
    Exit;
  
  // 연결 시도
  FSocket.Connect(FHost, IntToStr(FPort));
  FLastConnectAttempt := Now;
  
  Result := FSocket.LastError = 0;
end;

procedure TNetworkHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  LogItem: PNetLogQueueItem;
  List: TList;
begin
  if AsyncMode = amThread then
  begin
    // 비동기 모드: 큐에 메시지 추가
    New(LogItem);
    LogItem^.Message := Msg;
    LogItem^.Level := Level;

    List := FLogQueue.LockList; // 스레드 안전한 Lock 획득
    try
      List.Add(LogItem);

      // 큐 크기 확인
      if List.Count >= FQueueMaxSize then
        FAsyncThread.SignalQueue;
    finally
      FLogQueue.UnlockList; // Lock 해제
    end;
  end
  else
  begin
    // 동기 모드: 직접 네트워크로 전송
    SendToNetwork(Msg);
  end;
end;

procedure TNetworkHandler.WriteSessionStart;
var
  AppName: string;
  SessionStartMsg: string;
begin
  AppName := ExtractFileName(ParamStr(0));
  SessionStartMsg := Format('=== Log Session Started at %s by %s ===', 
                           [DateTimeToStr(Now), AppName]);
  
  // 세션 시작 메시지 기록
  WriteLog(SessionStartMsg, llInfo);
end;

procedure TNetworkHandler.SetAsyncMode(Mode: TAsyncMode);
begin
  // 현재 모드와 같으면 아무것도 하지 않음
  if Mode = AsyncMode then 
    Exit;
  
  inherited SetAsyncMode(Mode);
  
  case Mode of
    amNone:
      begin
        // 비동기 모드 비활성화
        if Assigned(FAsyncThread) then
        begin
          FAsyncThread.SignalTerminate;
          FAsyncThread.Terminate;
          FAsyncThread.WaitFor;
          FAsyncThread.Free;
          FAsyncThread := nil;
        end;
        
        // 큐에 남은 로그 처리
        FlushQueue;
      end;
      
    amThread:
      begin
        // 비동기 스레드 모드 활성화
        if not Assigned(FAsyncThread) then
          FAsyncThread := TNetworkHandlerThread.Create(Self);
      end;
  end;
end;

procedure TNetworkHandler.FlushQueue;
var
  List: TList;
  i: Integer;
  LogItem: PNetLogQueueItem;
begin
  List := FLogQueue.LockList;
  try
    if List.Count = 0 then
      Exit;
      
    // 소켓 연결 확인
    TryConnect;
    
    // 모든 큐 항목 처리
    for i := 0 to List.Count - 1 do
    begin
      LogItem := PNetLogQueueItem(List[i]);
      
      // 네트워크로 전송
      SendToNetwork(LogItem^.Message);
      
      // 메모리 해제
      Dispose(LogItem);
    end;
    
    // 처리 완료된 항목 제거
    List.Clear;
    
    // 마지막 플러시 시간 갱신
    FLastQueueFlush := Now;
  finally
    FLogQueue.UnlockList;
  end;
end;

procedure TNetworkHandler.ProcessLogQueue;
var
  CurrentTime: TDateTime;
  ElapsedMS: Int64;
  List: TList;
begin
  CurrentTime := Now;
  ElapsedMS := MilliSecondsBetween(CurrentTime, FLastQueueFlush);
  
  // 큐 크기 또는 시간 간격 확인
  List := FLogQueue.LockList;
  try
    if (List.Count > 0) and
       ((List.Count >= FQueueMaxSize) or (ElapsedMS >= FQueueFlushInterval)) then
    begin
      // 큐 플러시 필요
      FLogQueue.UnlockList;
      FlushQueue;
    end;
  finally
    if List <> nil then
      FLogQueue.UnlockList;
  end;
end;

function TNetworkHandler.SendToNetwork(const Msg: string): Boolean;
begin
  Result := False;
  
  if not Assigned(FSocket) then
    Exit;
  
  // 소켓 상태 확인 및 필요시 재연결
  if FSocket.LastError <> 0 then
    if not TryConnect then
      Exit;
  
  // 메시지 전송
  FSocket.SendString(Msg + #13#10);
  Result := FSocket.LastError = 0;
end;

end.
