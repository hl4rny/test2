unit BufferedHandler;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, DateUtils, LogHandlers, SyncObjs;

type
  // 전방 선언 (forward declaration)
  TBufferedHandler = class;

  // 플러시 스레드 클래스 선언
  TBufferedHandlerFlushThread = class(TThread)
  private
    FOwner: TBufferedHandler;
    FInterval: Integer;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TBufferedHandler; AInterval: Integer);
  end;

  // 버퍼링된 로그 항목
  TBufferedLogItem = record
    Message: string;
    Level: TLogLevel;
    Tag: string;
    Timestamp: TDateTime;
  end;
  PBufferedLogItem = ^TBufferedLogItem;
  
  { TBufferedHandler - 로그 버퍼링 래퍼 핸들러 }
  // 참고: 이 핸들러는 다른 핸들러를 래핑하여 버퍼링 기능을 추가하는 데코레이터 패턴을 구현합니다
  TBufferedHandler = class(TLogHandler)
  private
    FWrappedHandler: TLogHandler;    // 실제 로깅 작업을 수행할 핸들러
    FOwnsHandler: Boolean;           // 핸들러 소유 여부 (소멸자에서 해제할지)
    FBuffer: TThreadList;            // 로그 버퍼
    FBufferSize: Integer;            // 버퍼 크기
    FFlushInterval: Integer;         // 자동 플러시 간격(ms)
    FLastFlush: TDateTime;           // 마지막 플러시 시간
    FFlushOnDestroy: Boolean;        // 소멸 시 버퍼 플러시 여부
    FFlushOnFatal: Boolean;          // 치명적 오류 발생 시 플러시 여부
    FFlushThread: TBufferedHandlerFlushThread; // 자동 플러시 스레드
    FAutoFlush: Boolean;             // 자동 플러시 활성화 여부
    
    // 버퍼 플러시
    procedure FlushBuffer;
    
    // 시간 간격 체크
    procedure CheckFlushInterval;
    
  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;
    
  public
    // 생성자 - 기존 핸들러를 래핑하여 버퍼링 추가
    constructor Create(AHandler: TLogHandler; OwnsHandler: Boolean = True); reintroduce;
    destructor Destroy; override;
    
    procedure Init; override;
    procedure Shutdown; override;
    
    // 즉시 버퍼 플러시
    procedure Flush;
    
    // 자동 플러시 타이머 시작/중지
    procedure StartAutoFlush(Interval: Integer = 5000);
    procedure StopAutoFlush;
    
    // 속성
    property BufferSize: Integer read FBufferSize write FBufferSize;
    property FlushInterval: Integer read FFlushInterval write FFlushInterval;
    property FlushOnDestroy: Boolean read FFlushOnDestroy write FFlushOnDestroy;
    property FlushOnFatal: Boolean read FFlushOnFatal write FFlushOnFatal;
    property WrappedHandler: TLogHandler read FWrappedHandler;
    property AutoFlush: Boolean read FAutoFlush;
  end;

implementation

{ TBufferedHandler.TBufferFlushThread }

constructor TBufferedHandlerFlushThread.Create(AOwner: TBufferedHandler; AInterval: Integer);
begin
  inherited Create(True);  // 일시 중단 상태로 생성
  FreeOnTerminate := False;
  FOwner := AOwner;
  FInterval := AInterval;
  Start;  // 스레드 시작
end;

procedure TBufferedHandlerFlushThread.Execute;
begin
  while not Terminated do
  begin
    // 지정된 간격마다 깨어나서 버퍼 플러시 확인
    Sleep(FInterval);
    
    if Terminated then
      Break;
      
    // 시간 간격 체크하여 필요시 플러시
    FOwner.CheckFlushInterval;
  end;
end;

{ TBufferedHandler }

constructor TBufferedHandler.Create(AHandler: TLogHandler; OwnsHandler: Boolean);
begin
  inherited Create;
  
  // 기본 설정
  FWrappedHandler := AHandler;
  FOwnsHandler := OwnsHandler;
  FBuffer := TThreadList.Create;
  FBufferSize := 100;           // 기본값: 100 항목
  FFlushInterval := 5000;       // 기본값: 5초
  FLastFlush := Now;
  FFlushOnDestroy := True;
  FFlushOnFatal := True;
  FFlushThread := nil;
  FAutoFlush := False;
  
  // 래핑된 핸들러의 설정 복사
  if Assigned(FWrappedHandler) then
  begin
    LogLevels := FWrappedHandler.LogLevels;
    Active := FWrappedHandler.Active;
  end;
end;

destructor TBufferedHandler.Destroy;
begin
  Shutdown;
  
  // 버퍼 플러시 (설정에 따라)
  if FFlushOnDestroy then
    FlushBuffer;
  
  // 버퍼 정리
  FBuffer.Free;
  
  // 래핑된 핸들러 정리 (설정에 따라)
  if FOwnsHandler and Assigned(FWrappedHandler) then
    FWrappedHandler.Free;
  
  inherited;
end;

procedure TBufferedHandler.Init;
begin
  inherited;
  
  // 래핑된 핸들러 초기화
  if Assigned(FWrappedHandler) then
    FWrappedHandler.Init;
end;

procedure TBufferedHandler.Shutdown;
begin
  // 자동 플러시 중지
  StopAutoFlush;
  
  // 래핑된 핸들러 종료
  if Assigned(FWrappedHandler) then
    FWrappedHandler.Shutdown;
    
  inherited;
end;

procedure TBufferedHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  BufferItem: PBufferedLogItem;
  List: TList;
begin
  // 래핑된 핸들러가 없으면 아무것도 하지 않음
  if not Assigned(FWrappedHandler) then 
    Exit;
  
  // 버퍼 항목 생성
  New(BufferItem);
  BufferItem^.Message := Msg;
  BufferItem^.Level := Level;
  BufferItem^.Tag := '';
  BufferItem^.Timestamp := Now;
  
  // 버퍼에 항목 추가
  List := FBuffer.LockList;
  try
    List.Add(BufferItem);
    
    // 버퍼 크기 확인
    if List.Count >= FBufferSize then
    begin
      // 버퍼 크기 초과하면 플러시
      FBuffer.UnlockList;
      FlushBuffer;
    end
    else
      FBuffer.UnlockList;
      
    // 치명적 오류 시 즉시 플러시 (설정에 따라)
    if FFlushOnFatal and (Level = llFatal) then
      FlushBuffer;
  except
    FBuffer.UnlockList;
    Dispose(BufferItem);
    raise;
  end;
end;

procedure TBufferedHandler.FlushBuffer;
var
  List: TList;
  i: Integer;
  BufferItem: PBufferedLogItem;
begin
  // 래핑된 핸들러가 없으면 아무것도 하지 않음
  if not Assigned(FWrappedHandler) then 
    Exit;
  
  List := FBuffer.LockList;
  try
    if List.Count = 0 then
      Exit;
      
    // 모든 버퍼 항목 처리
    for i := 0 to List.Count - 1 do
    begin
      BufferItem := PBufferedLogItem(List[i]);
      
      // 래핑된 핸들러에 전달
      FWrappedHandler.Deliver(BufferItem^.Message, BufferItem^.Level, BufferItem^.Tag);
      
      // 메모리 해제
      Dispose(BufferItem);
    end;
    
    // 처리 완료된 항목 제거
    List.Clear;
    
    // 마지막 플러시 시간 갱신
    FLastFlush := Now;
  finally
    FBuffer.UnlockList;
  end;
end;

procedure TBufferedHandler.CheckFlushInterval;
var
  CurrentTime: TDateTime;
  ElapsedMS: Int64;
begin
  CurrentTime := Now;
  ElapsedMS := MilliSecondsBetween(CurrentTime, FLastFlush);
  
  // 시간 간격 확인
  if ElapsedMS >= FFlushInterval then
    FlushBuffer;
end;

procedure TBufferedHandler.Flush;
begin
  FlushBuffer;
end;

procedure TBufferedHandler.StartAutoFlush(Interval: Integer);
begin
  // 이미 활성화되어 있으면 중지 후 재시작
  if FAutoFlush then
    StopAutoFlush;
  
  FFlushInterval := Interval;
  FFlushThread := TBufferedHandlerFlushThread.Create(Self, FFlushInterval);
  FAutoFlush := True;
end;

procedure TBufferedHandler.StopAutoFlush;
begin
  if Assigned(FFlushThread) then
  begin
    TThread(FFlushThread).Terminate;
    TThread(FFlushThread).WaitFor;
    FFlushThread.Free;
    FFlushThread := nil;
  end;
  
  FAutoFlush := False;
end;

end.
