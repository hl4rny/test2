unit MemoHandler;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, StdCtrls, Forms, Math, LogHandlers;

type
  // 로그 아이템 구조체 (비동기 전달용)
  TLogItem = record
    Message: string;
  end;
  PLogItem = ^TLogItem;
  
  // 메모핸들러 FreeNotification 처리용 래퍼 클래스
  TMemoHandlerWrapper = class;
  
  { TMemoHandler - 메모 로그 핸들러 }
  TMemoHandler = class(TLogHandler)
  private
    FMemoComponent: TCustomMemo;  // 메모 컴포넌트
    FMemoLogger: TStrings;        // 메모 라인 객체
    FShowTime: Boolean;           // 시간 표시 여부
    FWrapper: TMemoHandlerWrapper; // FreeNotification 래퍼
    FMaxLines: Integer;           // 최대 라인 수
    FTruncateOldLines: Boolean;   // 오래된 라인 삭제 여부
    
    // 메모 동기화 및 조작 메서드
    procedure UpdateMemoReference;
    procedure MemoAsyncAppend(Data: PtrInt);
    procedure HandleMemoDestruction(AMemo: TCustomMemo);
    procedure TruncateLines;
    
  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;
    
  public
    constructor Create(AMemo: TCustomMemo); reintroduce;
    destructor Destroy; override;
    
    procedure Init; override;
    procedure Shutdown; override;
    
    // 세션 시작 마커 기록
    procedure WriteSessionStart;
    
    // 메모 참조 변경
    procedure SetMemo(AMemo: TCustomMemo);
    
    // 속성
    property ShowTime: Boolean read FShowTime write FShowTime;
    property MaxLines: Integer read FMaxLines write FMaxLines;
    property TruncateOldLines: Boolean read FTruncateOldLines write FTruncateOldLines;
  end;
  
  { TMemoHandlerWrapper - FreeNotification 처리용 래퍼 }
  TMemoHandlerWrapper = class(TComponent)
  private
    FHandler: TMemoHandler;
  public
    constructor Create(AOwner: TComponent); override;
    procedure Notification(AComponent: TComponent; Operation: TOperation); override;
    property Handler: TMemoHandler read FHandler write FHandler;
  end;

implementation

{ TMemoHandlerWrapper }

constructor TMemoHandlerWrapper.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);
end;

procedure TMemoHandlerWrapper.Notification(AComponent: TComponent; Operation: TOperation);
begin
  inherited Notification(AComponent, Operation);
  
  // 메모 컴포넌트가 해제될 때 핸들러에 알림
  if (Operation = opRemove) and Assigned(FHandler) then
  begin
    if AComponent is TCustomMemo then
      FHandler.HandleMemoDestruction(TCustomMemo(AComponent));
  end;
end;

{ TMemoHandler }

constructor TMemoHandler.Create(AMemo: TCustomMemo);
begin
  inherited Create;
  
  // 기본 설정
  FShowTime := False;
  FMaxLines := 1000;
  FTruncateOldLines := True;
  
  // 래퍼 생성 및 설정
  FWrapper := TMemoHandlerWrapper.Create(nil);
  FWrapper.Handler := Self;
  
  // 메모 컴포넌트 설정
  SetMemo(AMemo);
end;

destructor TMemoHandler.Destroy;
begin
  // 래퍼 해제
  if Assigned(FWrapper) then
  begin
    FWrapper.Handler := nil;
    FWrapper.Free;
  end;
  
  FMemoComponent := nil;
  FMemoLogger := nil;
  
  inherited;
end;

procedure TMemoHandler.Init;
begin
  inherited;
  
  // 메모 참조 갱신
  UpdateMemoReference;
end;

procedure TMemoHandler.Shutdown;
begin
  // 메모 연결 해제
  if Assigned(FMemoComponent) then
  begin
    FMemoComponent.RemoveFreeNotification(FWrapper);
    FMemoComponent := nil;
  end;
  
  FMemoLogger := nil;
  
  inherited;
end;

procedure TMemoHandler.UpdateMemoReference;
begin
  // 메모 참조 업데이트
  if Assigned(FMemoComponent) then
    FMemoLogger := FMemoComponent.Lines
  else
    FMemoLogger := nil;
end;

procedure TMemoHandler.HandleMemoDestruction(AMemo: TCustomMemo);
begin
  // 연결된 메모가 해제될 때 호출됨
  if FMemoComponent = AMemo then
  begin
    FMemoComponent := nil;
    FMemoLogger := nil;
  end;
end;

procedure TMemoHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  MsgToAdd: string;
  LogItem: PLogItem;
begin
  // 항상 최신 참조로 업데이트
  UpdateMemoReference;
  
  if not Assigned(FMemoLogger) then 
    Exit;
  
  try
    // 메시지 준비 (시간 표시 여부에 따라)
    if FShowTime then
      MsgToAdd := Msg
    else
    begin
      if (Length(Msg) > 26) and (Msg[1] = '[') then
        MsgToAdd := Copy(Msg, 27, Length(Msg))
      else
        MsgToAdd := Msg;
    end;
    
    // 메인 스레드에서 메모에 추가
    if GetCurrentThreadId <> MainThreadID then
    begin
      // 비동기 호출 - 메인 스레드에서 실행되도록 예약
      New(LogItem);
      try
        LogItem^.Message := MsgToAdd;
        Application.QueueAsyncCall(@MemoAsyncAppend, PtrInt(LogItem));
      except
        Dispose(LogItem);
        raise;
      end;
    end
    else
    begin
      // 직접 추가
      FMemoLogger.Add(MsgToAdd);
      
      // 라인 수 제한 적용
      if FTruncateOldLines and (FMemoLogger.Count > FMaxLines) then
        TruncateLines;
    end;
  except
    on E: Exception do
    begin
      // 예외 발생 시 메모 연결 재설정 시도
      FMemoLogger := nil;
      if Assigned(FMemoComponent) then
        FMemoLogger := FMemoComponent.Lines;
    end;
  end;
end;

procedure TMemoHandler.MemoAsyncAppend(Data: PtrInt);
var
  LogItem: PLogItem;
begin
  LogItem := PLogItem(Data);
  try
    // 다시 참조 업데이트
    UpdateMemoReference;
    
    if Assigned(FMemoLogger) then
    begin
      // 메시지 추가
      FMemoLogger.Add(LogItem^.Message);
      
      // 라인 수 제한 적용
      if FTruncateOldLines and (FMemoLogger.Count > FMaxLines) then
        TruncateLines;
    end;
  finally
    Dispose(LogItem);
  end;
end;

procedure TMemoHandler.WriteSessionStart;
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

procedure TMemoHandler.SetMemo(AMemo: TCustomMemo);
begin
  // 이전 메모 연결 해제
  if Assigned(FMemoComponent) and (FMemoComponent <> AMemo) then
    FMemoComponent.RemoveFreeNotification(FWrapper);
  
  // 새 메모 설정
  FMemoComponent := AMemo;
  
  if Assigned(AMemo) then
  begin
    // 메모가 해제될 때 알림 받도록 설정
    AMemo.FreeNotification(FWrapper);
    FMemoLogger := AMemo.Lines;
  end
  else
    FMemoLogger := nil;
end;

procedure TMemoHandler.TruncateLines;
var
  i, LinesToDelete: Integer;
begin
  if not Assigned(FMemoLogger) then Exit;
  
  // 지울 라인 수 계산 (전체의 20% 또는 FMaxLines/5)
  LinesToDelete := Max(FMemoLogger.Count - FMaxLines, FMaxLines div 5);
  
  // 라인 삭제
  FMemoLogger.BeginUpdate;
  try
    for i := 1 to LinesToDelete do
      FMemoLogger.Delete(0);
  finally
    FMemoLogger.EndUpdate;
  end;
end;

end.
