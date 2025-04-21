unit FileHandler;

{$mode objfpc}{$H+}

interface

uses
  Windows, Classes, SysUtils, DateUtils, LogHandlers, SyncObjs, zipper;

type
  // 로그 파일 회전 모드
  TLogRotationMode = (lrmDate, lrmSize, lrmBoth);

  // 로그 큐 아이템 (비동기 처리용)
  TLogQueueItem = record
    Message: string;
    Level: TLogLevel;
  end;
  PLogQueueItem = ^TLogQueueItem;

  // 로그 파일 압축 옵션
  TLogCompressionOptions = record
    Enabled: Boolean;          // 압축 활성화 여부
    AfterDays: Integer;        // 며칠 이상된 파일 압축
    DeleteOriginal: Boolean;   // 압축 후 원본 삭제 여부
  end;

  { TFileHandlerThread - 비동기 파일 로깅용 스레드 }
  TFileHandlerThread = class(TThread)
  private
    FOwner: TObject;          // 소유자 (TFileHandler)
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

  { TFileHandler - 파일 로그 핸들러 }
  TFileHandler = class(TLogHandler)
  private
    FLogPath: string;            // 로그 파일 경로
    FLogFilePrefix: string;      // 로그 파일 접두사
    FCurrentLogFile: string;     // 현재 로그 파일 경로
    FMaxFileSize: Int64;         // 최대 로그 파일 크기
    FRotationMode: TLogRotationMode; // 로그 파일 회전 모드
    FMaxLogFiles: Integer;       // 최대 로그 파일 수
    FLogFile: TextFile;          // 로그 파일 핸들
    FFileOpened: Boolean;        // 파일 열림 상태
    FCompression: TLogCompressionOptions; // 로그 압축 옵션

    // 비동기 처리 관련 필드
    FLogQueue: TThreadList;      // 로그 메시지 큐
    FAsyncThread: TFileHandlerThread; // 비동기 처리 스레드
    FQueueMaxSize: Integer;      // 큐 최대 크기
    FQueueFlushInterval: Integer; // 큐 자동 플러시 간격
    FLastQueueFlush: TDateTime;  // 마지막 큐 플러시 시간

    procedure CheckFileRotation;  // 로그 파일 회전 확인
    procedure CleanupOldLogFiles; // 오래된 로그 파일 정리
    procedure CompressOldLogFiles; // 오래된 로그 파일 압축
    procedure OpenLogFile;        // 로그 파일 열기
    procedure CloseLogFile;       // 로그 파일 닫기
    procedure FlushQueue;         // 큐에 있는 로그 처리
    procedure ProcessLogQueue;    // 로그 큐 처리 (비동기 스레드에서 호출)

  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;

  public
    constructor Create(const LogPath, FilePrefix: string); reintroduce;
    destructor Destroy; override;

    procedure Init; override;
    procedure Shutdown; override;

    // 세션 시작 마커 기록
    procedure WriteSessionStart;

    // 비동기 모드 설정 오버라이드
    procedure SetAsyncMode(Mode: TAsyncMode); override;

    // 로그 압축 설정
    procedure EnableCompression(Enable: Boolean = True; AfterDays: Integer = 7; DeleteOriginal: Boolean = True);

    // 속성
    property LogPath: string read FLogPath write FLogPath;
    property LogFilePrefix: string read FLogFilePrefix write FLogFilePrefix;
    property MaxFileSize: Int64 read FMaxFileSize write FMaxFileSize;
    property RotationMode: TLogRotationMode read FRotationMode write FRotationMode;
    property MaxLogFiles: Integer read FMaxLogFiles write FMaxLogFiles;
    property QueueMaxSize: Integer read FQueueMaxSize write FQueueMaxSize;
    property QueueFlushInterval: Integer read FQueueFlushInterval write FQueueFlushInterval;
  end;

implementation

{ TFileHandlerThread }

constructor TFileHandlerThread.Create(AOwner: TObject);
begin
  inherited Create(True); // 일시 중단 상태로 생성

  FOwner := AOwner;
  FQueueEvent := TEvent.Create(nil, False, False, '');
  FTerminateEvent := TEvent.Create(nil, True, False, '');

  FreeOnTerminate := False;

  // 스레드 시작
  Start;
end;

destructor TFileHandlerThread.Destroy;
begin
  FQueueEvent.Free;
  FTerminateEvent.Free;

  inherited;
end;

procedure TFileHandlerThread.Execute;
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
        TFileHandler(FOwner).ProcessLogQueue;
      WAIT_OBJECT_0 + 1: // FTerminateEvent 신호 (인덱스 1)
        Break;
      WAIT_TIMEOUT:      // 타임아웃 (5초 경과)
        TFileHandler(FOwner).ProcessLogQueue;
      else               // 오류 발생 (WAIT_FAILED 등)
        Break;
    end;
  end;
end;

procedure TFileHandlerThread.SignalQueue;
begin
  FQueueEvent.SetEvent;
end;

procedure TFileHandlerThread.SignalTerminate;
begin
  FTerminateEvent.SetEvent;
end;

{ TFileHandler }

// TFileHandler.Create 메서드 수정
constructor TFileHandler.Create(const LogPath, FilePrefix: string);
var
  BasePath, DatePath: string;
  CurrentDate: TDateTime;
begin
  inherited Create;

  // 경로가 공백이면 날짜별 디렉토리 구조 사용
  if Trim(LogPath) = '' then
  begin
    BasePath := ExtractFilePath(ParamStr(0)) + 'logs';
    CurrentDate := Date;
    DatePath := IncludeTrailingPathDelimiter(BasePath) +
                FormatDateTime('yyyy', CurrentDate) + PathDelim +
                FormatDateTime('mm', CurrentDate) + PathDelim +
                FormatDateTime('dd', CurrentDate);

    FLogPath := IncludeTrailingPathDelimiter(DatePath);
  end
  else
    FLogPath := IncludeTrailingPathDelimiter(LogPath);

  // 디렉토리가 없으면 생성
  if not DirectoryExists(FLogPath) then
    ForceDirectories(FLogPath);

  // 나머지 초기화 코드...
  FLogFilePrefix := FilePrefix;
  FCurrentLogFile := FLogPath + FLogFilePrefix + '_' + FormatDateTime('yyyymmdd', Date) + '.log';
  FMaxFileSize := 50 * 1024 * 1024; // 기본 50MB
  FMaxLogFiles := 10;
  FRotationMode := lrmDate;
  FFileOpened := False;

  // 압축 옵션
  FCompression.Enabled := False;
  FCompression.AfterDays := 7;
  FCompression.DeleteOriginal := True;

  // 비동기 처리 관련
  FLogQueue := TThreadList.Create;
  FAsyncThread := nil;
  FQueueMaxSize := 1000;       // 기본 큐 크기
  FQueueFlushInterval := 5000; // 기본 플러시 간격 (ms)
  FLastQueueFlush := Now;
end;

destructor TFileHandler.Destroy;
begin
  Shutdown;

  // 로그 큐 정리
  FlushQueue;
  FLogQueue.Free;

  inherited;
end;

procedure TFileHandler.Init;
begin
  inherited;

  // 로그 디렉토리 확인 및 생성
  if not DirectoryExists(FLogPath) then
    ForceDirectories(FLogPath);

  // 현재 로그 파일 설정
  FCurrentLogFile := FLogPath + FLogFilePrefix + '_' + FormatDateTime('yyyymmdd', Date) + '.log';
end;

procedure TFileHandler.Shutdown;
begin
  // 비동기 스레드 종료
  SetAsyncMode(amNone);

  // 로그 큐 플러시
  FlushQueue;

  // 파일 닫기
  CloseLogFile;

  inherited;
end;


procedure TFileHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  LogItem: PLogQueueItem;
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
    // 동기 모드: 직접 파일에 기록
    CheckFileRotation;

    if not FFileOpened then
      OpenLogFile;

    try
      WriteLn(FLogFile, Msg);
      Flush(FLogFile);
    except
      // 파일 쓰기 실패 처리
      CloseLogFile;
    end;
  end;
end;

procedure TFileHandler.WriteSessionStart;
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

procedure TFileHandler.SetAsyncMode(Mode: TAsyncMode);
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
          FAsyncThread := TFileHandlerThread.Create(Self);
      end;
  end;
end;

procedure TFileHandler.EnableCompression(Enable: Boolean; AfterDays: Integer; DeleteOriginal: Boolean);
begin
  FCompression.Enabled := Enable;
  FCompression.AfterDays := AfterDays;
  FCompression.DeleteOriginal := DeleteOriginal;

  // 즉시 압축 실행
  if Enable then
    CompressOldLogFiles;
end;

procedure TFileHandler.CheckFileRotation;
var
  FileInfo: TSearchRec;
  NewLogFile: string;
  NeedRotation: Boolean;
begin
  NeedRotation := False;

  if FRotationMode in [lrmDate, lrmBoth] then
  begin
    NewLogFile := FLogPath + FLogFilePrefix + '_' + FormatDateTime('yyyymmdd', Date) + '.log';
    if FCurrentLogFile <> NewLogFile then
    begin
      CloseLogFile;
      FCurrentLogFile := NewLogFile;
      NeedRotation := True;
    end;
  end;

  if (FRotationMode in [lrmSize, lrmBoth]) and (not NeedRotation) then
  begin
    if FindFirst(FCurrentLogFile, faAnyFile, FileInfo) = 0 then
    begin
      try
        if FileInfo.Size >= FMaxFileSize then
        begin
          CloseLogFile;
          NewLogFile := FLogPath + FLogFilePrefix + '_' +
                       FormatDateTime('yyyymmdd_hhnnss', Now) + '.log';
          FCurrentLogFile := NewLogFile;
          NeedRotation := True;
        end;
      finally
        FindClose(FileInfo);
      end;
    end;
  end;

  if NeedRotation then
  begin
    CleanupOldLogFiles;

    // 압축 옵션이 활성화되어 있으면 압축 실행
    if FCompression.Enabled then
      CompressOldLogFiles;
  end;
end;

procedure TFileHandler.CleanupOldLogFiles;
var
  LogFiles: TStringList;
  SearchRec: TSearchRec;
  i: Integer;
begin
  if FMaxLogFiles <= 0 then Exit;

  LogFiles := TStringList.Create;
  try
    // 모든 로그 파일 찾기 (.log와 .zip 모두)
    if FindFirst(FLogPath + FLogFilePrefix + '_*.*', faAnyFile, SearchRec) = 0 then
    begin
      try
        repeat
          if (LowerCase(ExtractFileExt(SearchRec.Name)) = '.log') or
             (LowerCase(ExtractFileExt(SearchRec.Name)) = '.zip') then
            LogFiles.Add(FLogPath + SearchRec.Name);
        until FindNext(SearchRec) <> 0;
      finally
        FindClose(SearchRec);
      end;
    end;

    // 날짜순으로 정렬 (가장 오래된 파일이 먼저 오도록)
    LogFiles.Sort;

    // 오래된 파일 삭제
    while LogFiles.Count > FMaxLogFiles do
    begin
      try
        DeleteFile(LogFiles[0]);
      except
        // 삭제 실패 처리
      end;
      LogFiles.Delete(0);
    end;
  finally
    LogFiles.Free;
  end;
end;

procedure TFileHandler.CompressOldLogFiles;
var
  SearchRec: TSearchRec;
  LogFileName, ZipFileName: string;
  FileDate: TDateTime;
  Zipper: TZipper;
  ZipFile: TZipFileEntry;
begin
  if not FCompression.Enabled then Exit;

  // 모든 로그 파일 검색
  if FindFirst(FLogPath + FLogFilePrefix + '_*.log', faAnyFile, SearchRec) = 0 then
  begin
    try
      repeat
        LogFileName := FLogPath + SearchRec.Name;

        // 현재 사용 중인 로그 파일은 건너뜀
        if LogFileName = FCurrentLogFile then
          Continue;

        // 파일 날짜 확인
        FileDate := FileDateToDateTime(SearchRec.Time);

        // 기준일 이전 파일만 압축
        if (Now - FileDate) > FCompression.AfterDays then
        begin
          ZipFileName := ChangeFileExt(LogFileName, '.zip');

          // 이미 압축 파일이 있는지 확인
          if not FileExists(ZipFileName) then
          begin
            try
              Zipper := TZipper.Create;
              try
                Zipper.FileName := ZipFileName;
                ZipFile := Zipper.Entries.AddFileEntry(LogFileName, ExtractFileName(LogFileName));
                Zipper.ZipAllFiles;
              finally
                Zipper.Free;
              end;

              // 압축 성공 시 원본 파일 삭제 (옵션에 따라)
              if FCompression.DeleteOriginal then
                DeleteFile(LogFileName);
            except
              // 압축 실패 처리
            end;
          end;
        end;
      until FindNext(SearchRec) <> 0;
    finally
      FindClose(SearchRec);
    end;
  end;
end;

procedure TFileHandler.OpenLogFile;
var
  LogDir: string;
begin
  if FFileOpened then Exit;

  LogDir := ExtractFilePath(FCurrentLogFile);
  if not DirectoryExists(LogDir) then
    ForceDirectories(LogDir);

  AssignFile(FLogFile, FCurrentLogFile);
  if FileExists(FCurrentLogFile) then
    Append(FLogFile)
  else
    Rewrite(FLogFile);

  FFileOpened := True;
end;

procedure TFileHandler.CloseLogFile;
begin
  if FFileOpened then
  begin
    CloseFile(FLogFile);
    FFileOpened := False;
  end;
end;

procedure TFileHandler.FlushQueue;
var
  List: TList;
  i: Integer;
  LogItem: PLogQueueItem;
begin
  List := FLogQueue.LockList;
  try
    if List.Count = 0 then
      Exit;

    // 로그 파일 열기
    if not FFileOpened then
      OpenLogFile;

    try
      // 모든 큐 항목 처리
      for i := 0 to List.Count - 1 do
      begin
        LogItem := PLogQueueItem(List[i]);

        // 파일에 기록
        WriteLn(FLogFile, LogItem^.Message);

        // 메모리 해제
        Dispose(LogItem);
      end;

      // 파일 버퍼 플러시
      Flush(FLogFile);

      // 처리 완료된 항목 제거
      List.Clear;

      // 마지막 플러시 시간 갱신
      FLastQueueFlush := Now;
    except
      // 파일 쓰기 실패 처리
      CloseLogFile;
    end;
  finally
    FLogQueue.UnlockList;
  end;
end;

procedure TFileHandler.ProcessLogQueue;
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
