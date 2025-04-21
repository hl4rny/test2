unit SerialHandler;

{$mode objfpc}{$H+}

interface

uses
  Windows, Classes, SysUtils, LogHandlers, SyncObjs, DateUtils,
  // ComPort 라이브러리 사용
  CPort;

type
  // 시리얼 로그 큐 아이템
  TSerialLogQueueItem = record
    Message: string;
    Level: TLogLevel;
  end;
  PSerialLogQueueItem = ^TSerialLogQueueItem;
  
  { TSerialHandlerThread - 시리얼 로깅용 스레드 }
  TSerialHandlerThread = class(TThread)
  private
    FOwner: TObject;          // 소유자 (TSerialHandler)
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
  
  { TSerialHandler - 시리얼 포트 로그 핸들러 }
  TSerialHandler = class(TLogHandler)
  private
    FSerialPort: TComPort;    // 시리얼 포트 객체
    FOwnsPort: Boolean;       // 포트 소유 여부
    FAddLineEnding: Boolean;  // 줄바꿈 문자 추가 여부
    FLineEnding: string;      // 줄바꿈 문자
    FMaxLineLength: Integer;  // 최대 라인 길이
    
    // 비동기 처리 관련 필드
    FLogQueue: TThreadList;      // 로그 메시지 큐
    FAsyncThread: TSerialHandlerThread; // 비동기 처리 스레드
    FQueueMaxSize: Integer;      // 큐 최대 크기
    FQueueFlushInterval: Integer; // 큐 자동 플러시 간격
    FLastQueueFlush: TDateTime;  // 마지막 큐 플러시 시간
    
    function SendToSerial(const Msg: string): Boolean; // 시리얼로 전송
    procedure FlushQueue;           // 큐에 있는 로그 처리
    procedure ProcessLogQueue;      // 로그 큐 처리 (비동기 스레드에서 호출)
    
  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;
    
  public
    // 생성자 - 기존 시리얼 포트 사용 또는 새로 생성
    constructor Create(ASerialPort: TComPort = nil; OwnsPort: Boolean = False); reintroduce;
    destructor Destroy; override;
    
    procedure Init; override;
    procedure Shutdown; override;
    
    // 세션 시작 마커 기록
    procedure WriteSessionStart;
    
    // 비동기 모드 설정 오버라이드
    procedure SetAsyncMode(Mode: TAsyncMode); override;
    
    // 시리얼 포트 설정 (이미 설정된 포트를 사용하지 않을 경우)
    procedure ConfigurePort(const PortName: string; BaudRate: TBaudRate = br9600;
                           DataBits: TDataBits = dbEight; StopBits: TStopBits = sbOneStopBit;
                           Parity: TParityBits = prNone; FlowControl: TFlowControl = fcNone);
    
    // 속성
    property SerialPort: TComPort read FSerialPort write FSerialPort;
    property OwnsPort: Boolean read FOwnsPort write FOwnsPort;
    property AddLineEnding: Boolean read FAddLineEnding write FAddLineEnding;
    property LineEnding: string read FLineEnding write FLineEnding;
    property MaxLineLength: Integer read FMaxLineLength write FMaxLineLength;
    property QueueMaxSize: Integer read FQueueMaxSize write FQueueMaxSize;
    property QueueFlushInterval: Integer read FQueueFlushInterval write FQueueFlushInterval;
  end;

implementation

{ TSerialHandlerThread }

constructor TSerialHandlerThread.Create(AOwner: TObject);
begin
  inherited Create(True); // 일시 중단 상태로 생성
  
  FOwner := AOwner;
  FQueueEvent := TEvent.Create(nil, False, False, '');
  FTerminateEvent := TEvent.Create(nil, True, False, '');
  
  FreeOnTerminate := False;
  
  // 스레드 시작
  Start;
end;

destructor TSerialHandlerThread.Destroy;
begin
  FQueueEvent.Free;
  FTerminateEvent.Free;
  
  inherited;
end;

procedure TSerialHandlerThread.Execute;
var
  WaitResult: DWORD;
  Events: array[0..1] of THandle;
begin
  Events[0] := THandle(FQueueEvent.Handle);     // 큐 이벤트 핸들
  Events[1] := THandle(FTerminateEvent.Handle); // 종료 이벤트 핸들

  while not Terminated do
  begin
    // 이벤트 대기 (5초 타임아웃)
    WaitResult := WaitForMultipleObjects(2, @Events[0], False, 5000);

    if Terminated then
      Break;

    case WaitResult of
      WAIT_OBJECT_0:     // FQueueEvent 신호 (인덱스 0)
        TSerialHandler(FOwner).ProcessLogQueue;
      WAIT_OBJECT_0 + 1: // FTerminateEvent 신호 (인덱스 1)
        Break;
      WAIT_TIMEOUT:      // 타임아웃 (5초 경과)
        TSerialHandler(FOwner).ProcessLogQueue;
      else               // 오류 발생 (WAIT_FAILED 등)
        Break;
    end;
  end;
end;

procedure TSerialHandlerThread.SignalQueue;
begin
  FQueueEvent.SetEvent;
end;

procedure TSerialHandlerThread.SignalTerminate;
begin
  FTerminateEvent.SetEvent;
end;

{ TSerialHandler }

constructor TSerialHandler.Create(ASerialPort: TComPort; OwnsPort: Boolean);
begin
  inherited Create;
  
  // 시리얼 포트 설정
  FSerialPort := ASerialPort;
  FOwnsPort := OwnsPort;
  FAddLineEnding := True;
  FLineEnding := #13#10;  // CRLF 기본값
  FMaxLineLength := 256;  // 기본값
  
  // 시리얼 포트가 없으면 새로 생성
  if not Assigned(FSerialPort) and FOwnsPort then
  begin
    FSerialPort := TComPort.Create(nil);
    FOwnsPort := True;
  end;
  
  // 비동기 처리 관련
  FLogQueue := TThreadList.Create;
  FAsyncThread := nil;
  FQueueMaxSize := 1000;       // 기본 큐 크기
  FQueueFlushInterval := 5000; // 기본 플러시 간격 (ms)
  FLastQueueFlush := Now;
end;

destructor TSerialHandler.Destroy;
begin
  Shutdown;
  
  // 로그 큐 정리
  FlushQueue;
  FLogQueue.Free;
  
  // 시리얼 포트 정리
  if FOwnsPort and Assigned(FSerialPort) then
    FSerialPort.Free;
  
  inherited;
end;

procedure TSerialHandler.Init;
begin
  inherited;
  
  // 시리얼 포트 열기
  if Assigned(FSerialPort) and not FSerialPort.Connected then
  try
    FSerialPort.Open;
  except
    on E: Exception do
    begin
      // 포트 열기 실패 처리
      // 여기서는 로깅할 수 없으므로 단순히 예외 무시
    end;
  end;
end;

procedure TSerialHandler.Shutdown;
begin
  // 비동기 스레드 종료
  SetAsyncMode(amNone);
  
  // 로그 큐 플러시
  FlushQueue;
  
  // 시리얼 포트 닫기 (소유한 경우)
  if FOwnsPort and Assigned(FSerialPort) and FSerialPort.Connected then
  try
    FSerialPort.Close;
  except
    // 닫기 실패 무시
  end;
  
  inherited;
end;

procedure TSerialHandler.ConfigurePort(const PortName: string; BaudRate: TBaudRate;
                                     DataBits: TDataBits; StopBits: TStopBits;
                                     Parity: TParityBits; FlowControl: TFlowControl);
begin
  // 포트가 없으면 무시
  if not Assigned(FSerialPort) then
    Exit;
  
  // 포트가 열려 있으면 닫기
  if FSerialPort.Connected then
    FSerialPort.Close;
  
  try
    // 시리얼 포트 설정
    FSerialPort.Port := PortName;
    FSerialPort.BaudRate := BaudRate;
    FSerialPort.DataBits := DataBits;
    FSerialPort.StopBits := StopBits;
    FSerialPort.Parity.Bits := Parity;
    FSerialPort.FlowControl.FlowControl := FlowControl;
    
    // 기본 흐름 제어 설정
    FSerialPort.FlowControl.ControlDTR := dtrDisable;
    FSerialPort.FlowControl.ControlRTS := rtsDisable;
    
    // 포트 열기
    FSerialPort.Open;
  except
    on E: Exception do
    begin
      // 설정 실패 처리
      // 여기서는 로깅할 수 없으므로 단순히 예외 무시
    end;
  end;
end;

procedure TSerialHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  LogItem: PSerialLogQueueItem;
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
    // 동기 모드: 직접 시리얼로 전송
    SendToSerial(Msg);
  end;
end;

procedure TSerialHandler.WriteSessionStart;
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

procedure TSerialHandler.SetAsyncMode(Mode: TAsyncMode);
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
          FAsyncThread := TSerialHandlerThread.Create(Self);
      end;
  end;
end;

function TSerialHandler.SendToSerial(const Msg: string): Boolean;
var
  SendMsg: AnsiString;
begin
  Result := False;
  
  // 시리얼 포트 확인
  if not Assigned(FSerialPort) or not FSerialPort.Connected then
    Exit;
  
  try
    // 메시지 준비
    if FAddLineEnding then
      SendMsg := Copy(Msg, 1, FMaxLineLength) + FLineEnding
    else
      SendMsg := Copy(Msg, 1, FMaxLineLength);
      
    // 시리얼로 전송
    FSerialPort.WriteStr(SendMsg);
    Result := True;
  except
    on E: Exception do
    begin
      // 전송 실패 처리
      Result := False;
    end;
  end;
end;

procedure TSerialHandler.FlushQueue;
var
  List: TList;
  i: Integer;
  LogItem: PSerialLogQueueItem;
begin
  List := FLogQueue.LockList;
  try
    if List.Count = 0 then
      Exit;
      
    // 모든 큐 항목 처리
    for i := 0 to List.Count - 1 do
    begin
      LogItem := PSerialLogQueueItem(List[i]);
      
      // 시리얼로 전송
      SendToSerial(LogItem^.Message);
      
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

procedure TSerialHandler.ProcessLogQueue;
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

end.
