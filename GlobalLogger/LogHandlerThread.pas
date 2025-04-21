unit LogHandlerThread;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, SyncObjs;

type
  { TLogHandlerThread - 모든 로그 핸들러 스레드의 기본 클래스 }
  TLogHandlerThread = class(TThread)
  private
    FOwner: TObject;           // 소유자 객체
    FQueueEvent: TEvent;       // 큐 처리 이벤트
    FTerminateEvent: TEvent;   // 종료 이벤트

    {$IFNDEF WINDOWS}
    FWaitCounter: Cardinal;    // 비Windows 플랫폼용 타이머 카운터
    {$ENDIF}

  protected
    procedure Execute; override;

  public
    constructor Create(AOwner: TObject);
    destructor Destroy; override;

    // 큐 처리 신호
    procedure SignalQueue;

    // 종료 신호
    procedure SignalTerminate;

    procedure DoOwnerProcessQueue;

    // 속성
    property Owner: TObject read FOwner;
  end;

implementation

{$IFDEF WINDOWS}
uses
  Windows;
{$ENDIF}

{ TLogHandlerThread }

constructor TLogHandlerThread.Create(AOwner: TObject);
begin
  inherited Create(True); // 일시 중단 상태로 생성

  FOwner := AOwner;
  FQueueEvent := TEvent.Create(nil, False, False, '');
  FTerminateEvent := TEvent.Create(nil, True, False, '');

  {$IFNDEF WINDOWS}
  FWaitCounter := 0;
  {$ENDIF}

  FreeOnTerminate := False;
end;

destructor TLogHandlerThread.Destroy;
begin
  // 이벤트 해제
  FQueueEvent.Free;
  FTerminateEvent.Free;

  inherited;
end;

procedure TLogHandlerThread.Execute;
{$IFDEF WINDOWS}
var
  WaitResult: DWORD;
  Events: array[0..1] of THandle;
{$ELSE}
var
  WaitResult: TWaitResult;
{$ENDIF}
begin
  {$IFDEF WINDOWS}
  // Windows 구현
  Events[0] := THandle(FQueueEvent.Handle); // 명시적 타입 캐스팅 추가
  Events[1] := THandle(FTerminateEvent.Handle); // 명시적 타입 캐스팅 추가

  while not Terminated do
  begin
    // 이벤트 대기
    WaitResult := Windows.WaitForMultipleObjects(2, @Events[0], False, 5000); // 5초 타임아웃

    if Terminated then
      Break;

    case WaitResult of
      WAIT_OBJECT_0:
        begin
          // 큐 처리 신호 - 안전하게 소유자의 ProcessQueue 직접 호출
          if Assigned(FOwner) and Assigned(FOwner.MethodAddress('ProcessQueue')) then
            TThread.Synchronize(Self, @TLogHandlerThread(Self).DoOwnerProcessQueue);
        end;
      WAIT_OBJECT_0 + 1:
        begin
          // 종료 신호
          Break;
        end;
      WAIT_TIMEOUT:
        begin
          // 주기적으로 큐 확인 - 안전하게 ProcessQueue 호출
          if Assigned(FOwner) and Assigned(FOwner.MethodAddress('ProcessQueue')) then
            TThread.Synchronize(Self, @TLogHandlerThread(Self).DoOwnerProcessQueue);
        end;
    end;
  end;
  {$ELSE}
  // Non-Windows 구현 (Linux, macOS 등)
  while not Terminated do
  begin
    // 500ms 간격으로 확인
    Sleep(500);

    // 이벤트 상태 확인
    if FQueueEvent.WaitFor(0) = wrSignaled then
    begin
      if Assigned(FOwner) and Assigned(FOwner.MethodAddress('ProcessQueue')) then
        TThread.Synchronize(Self, @TLogHandlerThread(Self).DoOwnerProcessQueue);
    end
    else if FTerminateEvent.WaitFor(0) = wrSignaled then
      Break
    else
    begin
      // 10회 확인 후 큐 처리 (대략 5초마다)
      Inc(FWaitCounter);
      if FWaitCounter >= 10 then
      begin
        if Assigned(FOwner) and Assigned(FOwner.MethodAddress('ProcessQueue')) then
          TThread.Synchronize(Self, @TLogHandlerThread(Self).DoOwnerProcessQueue);
        FWaitCounter := 0;
      end;
    end;
  end;
  {$ENDIF}
end;

procedure TLogHandlerThread.DoOwnerProcessQueue;
begin
  // 메인 스레드에서 안전하게 소유자의 ProcessQueue 메서드 호출
  if Assigned(FOwner) then
  begin
    // 소유자의 ProcessQueue 메서드를 직접 호출하는 대신
    // Synchronize를 사용해 메인 스레드에서 호출
    try
      // RTTI 사용하지 않고 컴파일러가 메서드 호출을 확인할 수 있게
      // 소유자 타입 확인 후 해당 메서드 호출
      if FOwner is TFileHandler then
        TFileHandler(FOwner).ProcessQueue
      else if FOwner is TNetworkHandler then
        TNetworkHandler(FOwner).ProcessQueue
      else if FOwner is TPrintfHandler then
        TPrintfHandler(FOwner).ProcessQueue
      else if FOwner is TSerialHandler then
        TSerialHandler(FOwner).ProcessQueue;
    except
      // 메서드 호출 오류 무시
    end;
  end;
end;

procedure TLogHandlerThread.SignalQueue;
begin
  FQueueEvent.SetEvent;
end;

procedure TLogHandlerThread.SignalTerminate;
begin
  FTerminateEvent.SetEvent;
end;

end.
