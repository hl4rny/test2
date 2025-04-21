unit PrintfHandler;

{$mode objfpc}{$H+}

interface

uses
  SysUtils, Classes, Controls, Forms, StdCtrls, ComCtrls, ExtCtrls,
  Graphics, Dialogs, Menus, Clipbrd, LMessages, LCLType, DateTimePicker,
  LogHandlers, SyncObjs, DateUtils,
  LCLProc, VirtualTrees; // VirtualTreeView 추가

type
  TPrintfHandler = class;

  // 검색 옵션
  TSearchOptions = record
    SearchText: string;
    MatchCase: Boolean;
    WholeWord: Boolean;
    SearchInTime: Boolean;
    SearchInLevel: Boolean;
    SearchInMessage: Boolean;
    SearchInSource: Boolean; // 소스 검색 옵션 추가
  end;

  // 필터 옵션
  TFilterOptions = record
    Enabled: Boolean;
    ShowInfo: Boolean;
    ShowDebug: Boolean;
    ShowWarning: Boolean;
    ShowError: Boolean;
    ShowFatal: Boolean;
    ShowDevelop: Boolean;
    TimeFrom: TDateTime;
    TimeTo: TDateTime;
    FilterTimeEnabled: Boolean;
    FilterText: string;
    FilterTextEnabled: Boolean;
    FilterTextMatchCase: Boolean;
    FilterSource: string; // 소스 필터 추가
    FilterSourceEnabled: Boolean; // 소스 필터 활성화 여부
  end;

  // 컬럼 가시성 옵션 추가
  TColumnVisibility = record
    ShowTimeColumn: Boolean;
    ShowLevelColumn: Boolean;
    ShowSourceColumn: Boolean;
    ShowMessageColumn: Boolean;
  end;

  // 폼 위치 옵션
  TFormPosition = (fpTopLeft, fpBottomLeft, fpTopRight, fpBottomRight, fpCustom);

  // 로그 추적 모드
  TTraceMode = (tmNormal, tmAutoScroll);

  // 로그 아이템 데이터 구조
  PLogData = ^TLogData;
  TLogData = record
    Message: string;
    Level: TLogLevel;
    Time: TDateTime;  // 로그 시간
    Source: string;   // 소스 정보
  end;

  // 로그 큐 아이템
  TPrintfLogQueueItem = record
    Message: string;
    Level: TLogLevel;
    Source: string;  // 소스 정보
  end;
  PPrintfLogQueueItem = ^TPrintfLogQueueItem;

  { TPrintfHandlerThread - 비동기 로깅용 스레드 }
  TPrintfHandlerThread = class(TThread)
  private
    FOwner: TObject;          // 소유자 (TPrintfHandler)
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

  // 소스 통계 폼 선언
  TSourceStatForm = class(TForm)
  private
    FListView: TListView;
    FCloseButton: TButton;
    FButtonPanel: TPanel;

    procedure CloseButtonClick(Sender: TObject);
  public
    constructor Create(AOwner: TComponent); reintroduce;
    procedure AddSourceItem(const Source: string; Count: Integer; ErrorCount: Integer);
    procedure ClearItems;
  end;

  // 소스 팝업 메뉴 클래스 선언
  TSourcePopupMenu = class(TPopupMenu)
  private
    FLogForm: TObject;
  public
    constructor Create(ALogForm: TObject); reintroduce;
    procedure FilterSourceClick(Sender: TObject);
  end;

  { TPrintfLogForm - 로그 표시 폼 (VirtualTreeView 사용) }
  TPrintfLogForm = class(TForm)
  private
    FLogTree: TVirtualStringTree;  // 로그 표시용 VirtualStringTree
    FStatusBar: TStatusBar;        // 상태바
    FTraceMode: TTraceMode;        // 추적 모드
    FOnClear: TNotifyEvent;        // 클리어 이벤트
    FOnSave: TNotifyEvent;         // 저장 이벤트
    FOwnerHandler: TPrintfHandler; // 소유 핸들러

    FTrayIcon: TTrayIcon;          // 시스템 트레이 아이콘

    // 검색 및 필터링 관련 필드
    FSearchPanel: TPanel;
    FFilterPanel: TPanel;
    FSearchEdit: TEdit;
    FSearchCaseSensitive: TCheckBox;
    FSearchWholeWord: TCheckBox;
    FSearchInTime: TCheckBox;
    FSearchInLevel: TCheckBox;
    FSearchInSource: TCheckBox;    // 소스 검색 체크박스
    FSearchInMessage: TCheckBox;
    FSearchButton: TButton;
    FCloseSearchButton: TButton;

    FFilterButton: TButton;
    FFilterInfoCheck: TCheckBox;
    FFilterDebugCheck: TCheckBox;
    FFilterWarningCheck: TCheckBox;
    FFilterErrorCheck: TCheckBox;
    FFilterFatalCheck: TCheckBox;
    FFilterDevelopCheck: TCheckBox;
    FFilterTimeFromCheck: TCheckBox;
    FFilterTimeFrom: TDateTimePicker;
    FFilterTimeToCheck: TCheckBox;
    FFilterTimeTo: TDateTimePicker;
    FFilterTextCheck: TCheckBox;
    FFilterText: TEdit;
    FFilterTextCaseSensitive: TCheckBox;
    // 소스 필터링 필드
    FFilterSourceCheck: TCheckBox;
    FFilterSource: TEdit;
    FFilterSourceCaseSensitive: TCheckBox;
    FApplyFilterButton: TButton;
    FResetFilterButton: TButton;
    FCloseFilterButton: TButton;

    // 컬럼 가시성 관련 필드
    FColumnVisibilityPanel: TPanel;
    FShowTimeColumnCheck: TCheckBox;
    FShowLevelColumnCheck: TCheckBox;
    FShowSourceColumnCheck: TCheckBox;
    FShowMessageColumnCheck: TCheckBox;
    FApplyColumnVisibilityButton: TButton;
    FCloseColumnVisibilityButton: TButton;

    FSearchOptions: TSearchOptions;
    FFilterOptions: TFilterOptions;
    FColumnVisibility: TColumnVisibility; // 컬럼 가시성 옵션
    FOriginalNodes: TList; // 필터링 전 모든 노드 참조 저장
    FIsFileViewMode: Boolean; // 파일 뷰 모드인지 여부
    FCurrentSearchNode: PVirtualNode; // 현재 검색 노드
    FUniqueSourceList: TStringList; // 고유 소스 목록 저장 (통계 및 자동완성용)

    // 자동 새로고침 관련 필드
    FAutoRefreshTimer: TTimer;
    FAutoRefreshEnabled: Boolean;
    FAutoRefreshInterval: Integer; // 밀리초 단위
    FLastLoadedFileName: string;
    FPreserveScrollPos: Boolean;
    FLastVisibleNode: PVirtualNode;
    FLastFileSize: Int64;              // 마지막으로 로드한 파일 크기
    FLastFileModified: TDateTime;      // 마지막 파일 수정 시간
    FLastLineBuffer: array of string;  // 마지막으로 읽은 파일의 마지막 몇 줄
    FFirstRefreshDone: Boolean;        // 첫 새로고침 완료 여부
    FSkipNextRefresh: Boolean;         // 다음 새로고침 건너뛰기 플래그

    procedure StatusBarDblClick(Sender: TObject);
    procedure CreatePopupMenu;
    procedure PopupMenuClick(Sender: TObject);
    procedure CopySelectedLogs;

    procedure FormClose(Sender: TObject; var CloseAction: TCloseAction);
    procedure FormKeyDown(Sender: TObject; var Key: Word; Shift: TShiftState);

    // 트레이 아이콘 이벤트 핸들러
    procedure TrayIconClick(Sender: TObject);
    procedure TrayIconDblClick(Sender: TObject);
    procedure CreateTrayIcon;

    // VirtualTreeView 관련 이벤트 핸들러
    procedure LogTreeGetText(Sender: TBaseVirtualTree; Node: PVirtualNode;
                             Column: TColumnIndex; TextType: TVSTTextType;
                             var CellText: String);
    procedure LogTreeGetNodeDataSize(Sender: TBaseVirtualTree;
                                    var NodeDataSize: Integer);
    procedure LogTreeFreeNode(Sender: TBaseVirtualTree; Node: PVirtualNode);
    procedure LogTreeDrawText(Sender: TBaseVirtualTree;
                             TargetCanvas: TCanvas; Node: PVirtualNode;
                             Column: TColumnIndex; const CellText: String;
                             const CellRect: TRect; var DefaultDraw: Boolean);
    procedure LogTreeHeaderClick(Sender: TVTHeader; HitInfo: TVTHeaderHitInfo);

    // 검색 및 필터링 관련
    procedure CreateSearchPanel;
    procedure CreateFilterPanel;
    // 컬럼 가시성 패널 생성
    procedure CreateColumnVisibilityPanel;
    procedure SearchButtonClick(Sender: TObject);
    procedure CloseSearchButtonClick(Sender: TObject);
    procedure FilterButtonClick(Sender: TObject);
    procedure ApplyFilterButtonClick(Sender: TObject);
    procedure ResetFilterButtonClick(Sender: TObject);
    procedure CloseFilterButtonClick(Sender: TObject);
    procedure FilterCheckChange(Sender: TObject);
    // 컬럼 가시성 관련
    procedure ColumnVisibilityButtonClick(Sender: TObject);
    procedure ApplyColumnVisibilityButtonClick(Sender: TObject);
    procedure CloseColumnVisibilityButtonClick(Sender: TObject);

    function MatchesFilter(Node: PVirtualNode): Boolean;
    function FindNextMatch(StartNode: PVirtualNode; SearchOptions: TSearchOptions): PVirtualNode;
    procedure HighlightSearchMatch(Node: PVirtualNode);
    procedure StoreOriginalNodes;
    procedure RestoreNodes;
    procedure ApplyFilter;
    // 컬럼 가시성 적용
    procedure ApplyColumnVisibility;

    // 소스 통계 관련
    procedure UpdateSourceStatistics;
    procedure ShowSourceStatistics;
    procedure AddSourceToList(const SourceStr: string);
  public
    constructor Create(AOwner: TComponent; AHandler: TPrintfHandler); reintroduce;
    destructor Destroy; override;

    // 파일 뷰 모드용 생성자
    constructor CreateForFileViewing(AOwner: TComponent; const FileName: string);

    // 로그 파일 로드 메서드
    procedure LoadLogsFromFile(const FileName: string);
    // 자동 새로고침 메뉴 추가
    procedure AddAutoRefreshMenuItem;
    // 스크롤 위치 유지 옵션 클릭 이벤트
    procedure OnPreserveScrollPosClick(Sender: TObject);
    // 자동 새로고침 메뉴 클릭 이벤트
    procedure OnAutoRefreshMenuClick(Sender: TObject);
    // 메뉴 항목 체크 상태 업데이트
    procedure UpdateAutoRefreshMenuChecks(SelectedInterval: Integer);
    // 타이머 이벤트 핸들러
    procedure OnAutoRefreshTimer(Sender: TObject);
    // 파일 정보 갱신 메서드
    procedure UpdateFileInfo(const FileName: string);
    // 파일 속성 변경 확인 메서드
    function HasFileAttributesChanged(const FileName: string): Boolean;
    // 수동 새로고침 메뉴 클릭 이벤트
    procedure OnManualRefreshClick(Sender: TObject);
    // 파일 변경 감지 함수 - 파일 끝 부분만 읽어서 비교
    function HasFileContentChanged(const FileName: string): Boolean;
    // 로그 파일 다시 로드 메서드
    procedure ReloadLogFile;
    // 파일 크기 가져오기 함수
    function GetFileSize(const FileName: string): Int64;


    // 날짜/시간 형식 검증 함수
    function IsValidDateTimeFormat(const DateTimeStr: string): Boolean;
    // 로그 레벨 검증 함수
    function IsValidLogLevel(const LevelStr: string): Boolean;
    // 문자열 로그 레벨을 TLogLevel로 변환
    function StringToLogLevel(const LevelStr: string): TLogLevel;
    // 기본 구분자 기반의 로그 라인 파싱 (백업 메서드)
    function ParseLogLineBasic(const LogLine: string;
                               out TimeStr, LevelStr, SourceStr, MsgStr: string): Boolean;


    // 검색 및 필터링 관련 메서드
    procedure ShowSearchPanel;
    procedure HideSearchPanel;
    procedure ShowFilterPanel;
    procedure HideFilterPanel;
    // 컬럼 가시성 패널 표시/숨기기 메서드
    procedure ShowColumnVisibilityPanel;
    procedure HideColumnVisibilityPanel;
    procedure FindNext;
    procedure FindPrev;

    // 로그 항목
    procedure AddLog(const LogText: string; Level: TLogLevel = llInfo; const Source: string = '');

    // 로그 저장
    procedure SaveLogs(const FileName: string = '');

    // 로그 클리어
    procedure ClearLogs;

    // 소스 통계 표시
    procedure ShowSourceStatisticsForm;

    // 속성
    property LogTree: TVirtualStringTree read FLogTree;
    property StatusBar: TStatusBar read FStatusBar;
    property TraceMode: TTraceMode read FTraceMode write FTraceMode;
    property OnClear: TNotifyEvent read FOnClear write FOnClear;
    property OnSave: TNotifyEvent read FOnSave write FOnSave;
    property ColumnVisibility: TColumnVisibility read FColumnVisibility write FColumnVisibility;
  end;

  { TPrintfHandler - 로그 출력용 핸들러 }
  TPrintfHandler = class(TLogHandler)
  private
    FLogForm: TPrintfLogForm;        // 로그 폼
    FVisible: Boolean;               // 폼 표시 여부
    FFormPosition: TFormPosition;    // 폼 위치
    FFormWidth: Integer;             // 폼 너비
    FFormHeight: Integer;            // 폼 높이
    FFormCaption: string;            // 폼 캡션
    FStayOnTop: Boolean;             // 항상 위에 표시 여부
    FAutoSave: Boolean;              // 자동 저장 여부
    FShowTime: Boolean;              // 시간 표시 여부
    FMaxLogCount: Integer;           // 최대 로그 개수
    FColorByLevel: Boolean;          // 레벨별 색상 적용 여부
    FColorBySource: Boolean;         // 소스별 색상 적용 여부

    // 비동기 처리 관련 필드
    FLogQueue: TThreadList;           // 로그 메시지 큐
    FAsyncThread: TPrintfHandlerThread; // 비동기 처리 스레드
    FQueueMaxSize: Integer;           // 큐 최대 크기
    FQueueFlushInterval: Integer;     // 큐 자동 플러시 간격
    FLastQueueFlush: TDateTime;       // 마지막 큐 플러시 시간

    // 로그 로테이션 관련 필드
    FAutoRotate: Boolean;        // 자동 로테이션 여부
    FRotateLineCount: Integer;   // 로테이션 기준 라인 수
    FRotateSaveBeforeClear: Boolean; // 클리어 전 저장 여부
    FRotateLogFolder: string;    // 로그 저장 폴더

    FShowDateInTimeColumn: Boolean; // 시간 컬럼에 날짜 표시 여부
    FColumnVisibility: TColumnVisibility; // 컬럼 가시성 설정

    // 소스 색상 맵
    FSourceColorMap: TStringList; // 소스별 색상 저장

    // 폼 위치 설정
    procedure SetFormPosition(Value: TFormPosition);

    // 폼 크기 설정
    procedure SetFormWidth(Value: Integer);
    procedure SetFormHeight(Value: Integer);

    // 폼 캡션 설정
    procedure SetFormCaption(Value: string);

    // 항상 위에 표시 설정
    procedure SetStayOnTop(Value: Boolean);

    // 로그 폼 표시 설정
    procedure SetVisible(Value: Boolean);
    // 가시성 상태 관리
    procedure UpdateFormVisibility(Visiblity: Boolean);

    // 색상 관련
    function GetColorForLevel(Level: TLogLevel): TColor;
    function GetColorForSource(const Source: string): TColor; // 소스별 색상

    // 로그 큐 처리
    procedure FlushQueue;
    procedure ProcessLogQueue;

    // 로그 폼 이벤트 핸들러
    procedure HandleFormClear(Sender: TObject);
    procedure HandleFormSave(Sender: TObject);

    // 컬럼 가시성 설정
    procedure SetColumnVisibility(const Value: TColumnVisibility);

    // 현재 로깅 중인 파일 열기 기능
    procedure ViewCurrentLogFile;
    // 가장 최근의 로그 파일 찾기
    function FindMostRecentLogFile(const LogFolder: string): string;
    // TPrintfHandler 클래스 내 메인 메뉴에 항목 추가
    procedure AddViewCurrentLogMenuItem;
    // 메뉴 클릭 이벤트 핸들러
    procedure ViewCurrentLogMenuClick(Sender: TObject);

  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;

  public
    constructor Create; override;
    destructor Destroy; override;

    procedure Init; override;
    procedure Shutdown; override;

    // 세션 시작 마커 기록
    procedure WriteSessionStart;

    // 비동기 모드 설정 오버라이드
    procedure SetAsyncMode(Mode: TAsyncMode); override;

    // 폼 디자인 설정
    procedure SetFormDesign(const ACaption: string; AWidth, AHeight: Integer;
                           APosition: TFormPosition = fpTopRight);

    // 폼 표시 설정
    procedure ShowForm(Show: Boolean = True);

    // 폰트 설정
    procedure SetFont(const FontName: string; FontSize: Integer; FontColor: TColor);

    // 배경색 설정
    procedure SetBackgroundColor(Color: TColor);

    // 로그 내용 저장
    procedure SaveToFile(const FileName: string = '');

    // 로그 내용 지우기
    procedure ClearLog;

    // 저장된 로그 파일 열기
    class procedure OpenLogFile(AOwner: TComponent);

    // 소스별 색상 설정 메서드 추가
    procedure SetSourceColor(const Source: string; Color: TColor);
    procedure ClearSourceColors;

    // 소스 통계 표시
    procedure ShowSourceStatistics;

    // 컬럼 가시성 설정
    procedure ConfigureColumnVisibility(ShowTime, ShowLevel, ShowSource, ShowMessage: Boolean);

    // 속성
    property LogForm: TPrintfLogForm read FLogForm;
    property Visible: Boolean read FVisible write SetVisible;
    property FormPosition: TFormPosition read FFormPosition write SetFormPosition;
    property FormWidth: Integer read FFormWidth write SetFormWidth;
    property FormHeight: Integer read FFormHeight write SetFormHeight;
    property FormCaption: string read FFormCaption write SetFormCaption;
    property StayOnTop: Boolean read FStayOnTop write SetStayOnTop;
    property AutoSave: Boolean read FAutoSave write FAutoSave;
    property ShowTime: Boolean read FShowTime write FShowTime;
    property MaxLogCount: Integer read FMaxLogCount write FMaxLogCount;
    property ColorByLevel: Boolean read FColorByLevel write FColorByLevel;
    property ColorBySource: Boolean read FColorBySource write FColorBySource; // 소스별 색상 속성 추가
    property QueueMaxSize: Integer read FQueueMaxSize write FQueueMaxSize;
    property QueueFlushInterval: Integer read FQueueFlushInterval write FQueueFlushInterval;
    property ShowDateInTimeColumn: Boolean read FShowDateInTimeColumn write FShowDateInTimeColumn;
    property ColumnVisibility: TColumnVisibility read FColumnVisibility write SetColumnVisibility;

    // 로그 로테이션 관련 속성
    property AutoRotate: Boolean read FAutoRotate write FAutoRotate;
    property RotateLineCount: Integer read FRotateLineCount write FRotateLineCount;
    property RotateSaveBeforeClear: Boolean read FRotateSaveBeforeClear write FRotateSaveBeforeClear;
    property RotateLogFolder: string read FRotateLogFolder write FRotateLogFolder;
  end;

implementation

{$IFDEF WINDOWS}
uses Windows;
{$ENDIF}


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


{ TSourceStatForm }

constructor TSourceStatForm.Create(AOwner: TComponent);
begin
  inherited CreateNew(AOwner);

  Caption := '소스별 로그 통계';
  Width := 450;         // 폼 너비 줄임 (500 -> 450)
  Height := 300;        // 폼 높이 줄임 (400 -> 300)
  Position := poScreenCenter;
  BorderStyle := bsSizeable;

  // 리스트뷰 생성
  FListView := TListView.Create(Self);
  with FListView do
  begin
    Parent := Self;
    Align := alClient;
    ViewStyle := vsReport;
    GridLines := True;
    RowSelect := True;
    ReadOnly := True;

    // 컬럼 설정 수정
    with Columns.Add do
    begin
      Caption := '소스';
      Width := 150;     // 소스 컬럼 너비 줄임 (200 -> 150)
    end;
    with Columns.Add do
    begin
      Caption := '로그 수';
      Width := 80;
      Alignment := taRightJustify;
    end;
    with Columns.Add do
    begin
      Caption := '오류/경고 수';
      Width := 100;
      Alignment := taRightJustify;
    end;
    with Columns.Add do
    begin
      Caption := '비율 (%)';
      Width := 80;
      Alignment := taRightJustify;
    end;
  end;

  // 닫기 버튼 위치 수정
  FCloseButton := TButton.Create(Self);
  with FCloseButton do
  begin
    Parent := Self;
    Caption := '닫기';
    ModalResult := mrOk;
    Default := True;
    Cancel := True;
    Width := 80;
    Height := 30;

    // 버튼 위치를 폼 하단 중앙으로 수정
    Left := (Width - 80) div 2;        // 폼 중앙에 배치
    Top := Height - 40;                // 하단에서 40픽셀 위
    Anchors := [akBottom];             // 하단에 고정

    OnClick := @CloseButtonClick;
  end;

  // 버튼 패널 추가 (닫기 버튼을 담을 패널)
  FButtonPanel := TPanel.Create(Self);
  with FButtonPanel do
  begin
    Parent := Self;
    Align := alBottom;
    Height := 40;
    BevelOuter := bvNone;

    // 닫기 버튼을 패널로 이동
    FCloseButton.Parent := FButtonPanel;
    FCloseButton.Anchors := [akTop, akRight];
    FCloseButton.Left := Width - 90;
    FCloseButton.Top := 5;
  end;
end;

procedure TSourceStatForm.CloseButtonClick(Sender: TObject);
begin
  Close;
end;

procedure TSourceStatForm.AddSourceItem(const Source: string; Count: Integer; ErrorCount: Integer);
var
  Item: TListItem;
  Percentage: Double;
begin
  Item := FListView.Items.Add;
  Item.Caption := Source;
  Item.SubItems.Add(IntToStr(Count));
  Item.SubItems.Add(IntToStr(ErrorCount));

  if Count > 0 then
    Percentage := (ErrorCount / Count) * 100
  else
    Percentage := 0;

  Item.SubItems.Add(Format('%.2f', [Percentage]));
end;

procedure TSourceStatForm.ClearItems;
begin
  FListView.Items.Clear;
end;

{ TSourcePopupMenu }

constructor TSourcePopupMenu.Create(ALogForm: TObject);
begin
  inherited Create(TComponent(ALogForm));
  FLogForm := ALogForm;
end;

procedure TSourcePopupMenu.FilterSourceClick(Sender: TObject);
var
  MenuItem: TMenuItem;
  SourceText: string;
  LogForm: TPrintfLogForm;
begin
  if not (Sender is TMenuItem) then
    Exit;

  LogForm := TPrintfLogForm(FLogForm);
  if not Assigned(LogForm) then
    Exit;

  MenuItem := TMenuItem(Sender);
  SourceText := MenuItem.Caption;

  // 필터 패널 표시
  LogForm.ShowFilterPanel;

  // 소스 필터 설정
  LogForm.FFilterSourceCheck.Checked := True;
  LogForm.FFilterSource.Text := SourceText;
  LogForm.FFilterSource.Enabled := True;
  LogForm.FFilterSourceCaseSensitive.Enabled := True;

  // 필터 적용
  LogForm.ApplyFilterButtonClick(nil);
end;

{ TPrintfHandlerThread }

constructor TPrintfHandlerThread.Create(AOwner: TObject);
begin
  inherited Create(True); // 일시 중단 상태로 생성

  FOwner := AOwner;
  FQueueEvent := TEvent.Create(nil, False, False, '');
  FTerminateEvent := TEvent.Create(nil, True, False, '');

  FreeOnTerminate := False;

  // 스레드 시작
  Start;
end;

destructor TPrintfHandlerThread.Destroy;
begin
  FQueueEvent.Free;
  FTerminateEvent.Free;

  inherited;
end;

procedure TPrintfHandlerThread.Execute;
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
        TPrintfHandler(FOwner).ProcessLogQueue;
      WAIT_OBJECT_0 + 1: // FTerminateEvent 신호 (인덱스 1)
        Break;
      WAIT_TIMEOUT:      // 타임아웃 (5초 경과)
        TPrintfHandler(FOwner).ProcessLogQueue;
      else               // 오류 발생 (WAIT_FAILED 등)
        Break;
    end;
  end;
end;

procedure TPrintfHandlerThread.SignalQueue;
begin
  FQueueEvent.SetEvent;
end;

procedure TPrintfHandlerThread.SignalTerminate;
begin
  FTerminateEvent.SetEvent;
end;

{ TPrintfLogForm }

constructor TPrintfLogForm.Create(AOwner: TComponent; AHandler: TPrintfHandler);
begin
  inherited CreateNew(AOwner);
  FOwnerHandler := AHandler;

  // 기본 폼 설정
  Caption := '로그 출력';
  Width := 700; // 폼 너비 증가
  Height := 500; // 폼 높이 증가
  Position := poDesigned;
  BorderStyle := bsSizeable;

  // 고유 소스 목록 초기화
  FUniqueSourceList := TStringList.Create;
  FUniqueSourceList.Sorted := True;
  FUniqueSourceList.Duplicates := dupIgnore;

  // 상태바 생성
  FStatusBar := TStatusBar.Create(Self);
  with FStatusBar do
  begin
    Parent := Self;
    SimplePanel := False;
    Panels.Add;
    Panels.Add;
    Panels[0].Width := 300; // 너비 증가
    Panels[0].Text := '윈도우 상태: 표준';
    Panels[1].Width := 300; // 너비 증가
    Panels[1].Text := '더블클릭: 자동 스크롤 활성화';
    OnDblClick := @StatusBarDblClick;
  end;

  // VirtualStringTree 생성
  FLogTree := TVirtualStringTree.Create(Self);

  with FLogTree do
  begin
    Parent := Self;
    Align := alClient;

    // 기본 설정
    Font.Name := '맑은 고딕';
    Font.Size := 9;
    Color := $00644300;  // 어두운 색상
    Font.Color := clSilver;

    // VirtualTreeView 설정
    Header.Options := [hoColumnResize, hoVisible, hoHotTrack];
    Header.Height := 20;

    // 중요: 전체 행 선택 설정
    TreeOptions.SelectionOptions := [toFullRowSelect, toMultiSelect, toRightClickSelect];

    // 기타 옵션 설정
    TreeOptions.MiscOptions := TreeOptions.MiscOptions + [toFullRepaintOnResize, toGridExtensions, toWheelPanning];
    TreeOptions.PaintOptions := TreeOptions.PaintOptions + [toShowVertGridLines, toShowHorzGridLines, toThemeAware];

    // 이벤트 설정
    OnGetText := @LogTreeGetText;
    OnGetNodeDataSize := @LogTreeGetNodeDataSize;
    OnFreeNode := @LogTreeFreeNode;
    OnDrawText := @LogTreeDrawText;
    OnHeaderClick := @LogTreeHeaderClick;

    // 컬럼 설정
    Header.Columns.Clear;

    // 시간 컬럼 - hh:nn:ss.zzz가 모두 보이도록 충분히 넓게 설정
    with Header.Columns.Add do
    begin
      Text := '시간';
      Width := 105;  // 밀리초까지 보이도록 더 넓게 설정
      MinWidth := 105;
      Alignment := taLeftJustify;
      Options := [coAllowClick, coDraggable, coEnabled, coParentBidiMode,
                  coParentColor, coResizable, coVisible, coFixed];
    end;

    // 레벨 컬럼 - 현재 적당함
    with Header.Columns.Add do
    begin
      Text := '레벨';
      Width := 65;
      MinWidth := 55;
      Alignment := taLeftJustify;
      Options := [coAllowClick, coDraggable, coEnabled, coParentBidiMode,
                  coParentColor, coResizable, coVisible, coFixed];
    end;

    // 소스 컬럼 - 더 좁게 조정
    with Header.Columns.Add do
    begin
      Text := '소스';
      Width := 65;  // 더 좁게 설정
      MinWidth := 55;
      Alignment := taLeftJustify;
      Options := [coAllowClick, coDraggable, coEnabled, coParentBidiMode,
                  coParentColor, coResizable, coVisible, coFixed];
    end;

    // 메시지 컬럼 - 더 넓게
    with Header.Columns.Add do
    begin
      Text := '메시지';
      Width := 440;
      MinWidth := 200;
      Alignment := taLeftJustify;
      Options := [coAllowClick, coDraggable, coEnabled, coParentBidiMode,
                  coParentColor, coResizable, coVisible]; // coFixed 제외
    end;

    // 컬럼 설정 적용
    Header.AutoSizeIndex := 3; // 메시지 컬럼이 확장되도록 설정

    // 노드 라인 높이 설정
    DefaultNodeHeight := 18;

    // 강제 갱신
    Invalidate;

    // 추가로 컬럼 너비를 초기 설정대로 강제 적용
    BeginUpdate;
    try
      // 각 컬럼의 너비를 명시적으로 설정하고 개별 갱신
      Header.Columns[0].Width := 105;  // 시간 (밀리초까지 표시)
      InvalidateColumn(0);

      Header.Columns[1].Width := 65;  // 레벨 (적당함)
      InvalidateColumn(1);

      Header.Columns[2].Width := 65;  // 소스 (더 좁게)
      InvalidateColumn(2);

      // 메시지 컬럼은 자동 크기 조정됨
      InvalidateColumn(3);
    finally
      EndUpdate;
    end;
  end;


  // 컬럼 가시성 초기 설정
  FColumnVisibility.ShowTimeColumn := True;
  FColumnVisibility.ShowLevelColumn := True;
  FColumnVisibility.ShowSourceColumn := True;  // 기본적으로 소스 컬럼 표시
  FColumnVisibility.ShowMessageColumn := True;

  // 팝업 메뉴 생성
  CreatePopupMenu;

  // 트레이 아이콘 생성
  CreateTrayIcon;

  // 패널 생성
  CreateSearchPanel;
  CreateFilterPanel;
  CreateColumnVisibilityPanel;

  // 기본값 설정
  FTraceMode := tmAutoScroll;

  // 단축키 설정 - 메인 메뉴 없이 단축키 사용
  KeyPreview := True;
  OnKeyDown := @FormKeyDown;
  OnClose := @FormClose;

  // 검색 옵션 초기화
  FSearchOptions.SearchInSource := True;  // 소스 컬럼 검색 활성화

  // 확인을 위한 디버그 코드 - 이 코드는 초기화 후 실행되어야 함
  StatusBar.Panels[0].Text := Format('칼럼 수: %d', [FLogTree.Header.Columns.Count]);
  // 상태바에 단축키 힌트 표시
  FStatusBar.Panels[1].Text := 'Ctrl+S: 저장, Ctrl+L: 지우기, Ctrl+V: 컬럼 표시/숨김, Ctrl+G: 소스통계';
end;

destructor TPrintfLogForm.Destroy;
begin
  // 마지막 라인 버퍼 해제
  SetLength(FLastLineBuffer, 0);

  // 타이머 해제
  if Assigned(FAutoRefreshTimer) then
  begin
    FAutoRefreshTimer.Enabled := False;
    FreeAndNil(FAutoRefreshTimer);
  end;

  // 원본 노드 리스트 해제
  if Assigned(FOriginalNodes) then
    FOriginalNodes.Free;

  // 소스 목록 해제
  if Assigned(FUniqueSourceList) then
    FUniqueSourceList.Free;

  // 필요한 다른 정리 작업...

  inherited;
end;


procedure TPrintfLogForm.CreateTrayIcon;
var
  TrayPopupMenu: TPopupMenu;
  MenuItem: TMenuItem;
  IconHandle: TIcon;
begin
  // 트레이 아이콘 생성
  FTrayIcon := TTrayIcon.Create(Self);

  // 아이콘 생성 및 설정
   IconHandle := TIcon.Create;
   try
     // 리소스에서 아이콘 로드
     IconHandle.LoadFromResourceName(HInstance, 'LOGVIEWER_ICON');

     // 파일에서 아이콘 로드
     // IconHandle.LoadFromFile('logviewer.ico');

     FTrayIcon.Icon.Assign(IconHandle);
   finally
     IconHandle.Free;
   end;

  // 트레이 아이콘 기본 설정 {16x16 ico 파일만 된다. 명심}
  // 아이콘 파일을 프로젝트에 추가할 때는 .ico 파일을 프로젝트 폴더에 넣고, 프로젝트 옵션에서 리소스로 추가함.
  FTrayIcon.Visible := True;
  FTrayIcon.Hint := '로그 뷰어';
  FTrayIcon.BalloonTitle := '로그 뷰어';
  FTrayIcon.BalloonHint := '로그 뷰어가 실행 중입니다. 클릭하여 보기/숨기기를 토글할 수 있습니다.';

  // 아이콘 설정 (기본 아이콘 사용)
  //FTrayIcon.Icon.Assign(Application.Icon);

  // 클릭 이벤트 설정
  FTrayIcon.OnClick := @TrayIconClick;
  FTrayIcon.OnDblClick := @TrayIconDblClick;

  // 트레이 아이콘 전용 팝업 메뉴
  TrayPopupMenu := TPopupMenu.Create(Self);

  // 로그 보이기/숨기기 메뉴
  MenuItem := TMenuItem.Create(TrayPopupMenu);
  MenuItem.Caption := '로그 창 보이기';
  MenuItem.Tag := 1;
  MenuItem.OnClick := @TrayIconClick;
  TrayPopupMenu.Items.Add(MenuItem);

  // 구분선
  MenuItem := TMenuItem.Create(TrayPopupMenu);
  MenuItem.Caption := '-';
  TrayPopupMenu.Items.Add(MenuItem);

  // 로그 지우기 메뉴
  MenuItem := TMenuItem.Create(TrayPopupMenu);
  MenuItem.Caption := '로그 지우기';
  MenuItem.Tag := 2;
  MenuItem.OnClick := @PopupMenuClick;
  TrayPopupMenu.Items.Add(MenuItem);

  // 로그 저장 메뉴
  MenuItem := TMenuItem.Create(TrayPopupMenu);
  MenuItem.Caption := '로그 저장...';
  MenuItem.Tag := 3;
  MenuItem.OnClick := @PopupMenuClick;
  TrayPopupMenu.Items.Add(MenuItem);

  // 구분선
  MenuItem := TMenuItem.Create(TrayPopupMenu);
  MenuItem.Caption := '-';
  TrayPopupMenu.Items.Add(MenuItem);

  // 소스 통계 메뉴
  MenuItem := TMenuItem.Create(TrayPopupMenu);
  MenuItem.Caption := '소스별 통계 보기...';
  MenuItem.Tag := 6;
  MenuItem.OnClick := @PopupMenuClick;
  TrayPopupMenu.Items.Add(MenuItem);

  // 구분선
  MenuItem := TMenuItem.Create(TrayPopupMenu);
  MenuItem.Caption := '-';
  TrayPopupMenu.Items.Add(MenuItem);

  // 종료 메뉴
  MenuItem := TMenuItem.Create(TrayPopupMenu);
  MenuItem.Caption := '로그 창 종료';
  MenuItem.Tag := 4;
  MenuItem.OnClick := @PopupMenuClick;
  TrayPopupMenu.Items.Add(MenuItem);

  // 팝업 메뉴 설정
  FTrayIcon.PopupMenu := TrayPopupMenu;
end;

procedure TPrintfLogForm.TrayIconClick(Sender: TObject);
begin
  // 로그 폼 표시/숨기기 토글
  if Visible then
  begin
    Hide;
    if Sender is TMenuItem then
      TMenuItem(Sender).Caption := '로그 창 보이기';
    // 트레이 아이콘 메뉴 캡션 업데이트
    if Assigned(FTrayIcon) and Assigned(FTrayIcon.PopupMenu) and
       (FTrayIcon.PopupMenu.Items.Count > 0) then
    begin
      FTrayIcon.PopupMenu.Items[0].Caption := '로그 창 보이기';
    end;
  end
  else
  begin
    Show;
    BringToFront;
    if Sender is TMenuItem then
      TMenuItem(Sender).Caption := '로그 창 숨기기';
    // 트레이 아이콘 메뉴 캡션 업데이트
    if Assigned(FTrayIcon) and Assigned(FTrayIcon.PopupMenu) and
       (FTrayIcon.PopupMenu.Items.Count > 0) then
    begin
      FTrayIcon.PopupMenu.Items[0].Caption := '로그 창 숨기기';
    end;
  end;

  // 소유자 핸들러에게 가시성 변경 알림
  if Assigned(FOwnerHandler) then
    FOwnerHandler.UpdateFormVisibility(Visible);
end;

procedure TPrintfLogForm.TrayIconDblClick(Sender: TObject);
begin
  // 더블클릭 시 로그 폼 표시/숨기기 토글 (click과 동일하게 동작)
  TrayIconClick(Sender);
end;

procedure TPrintfLogForm.FormClose(Sender: TObject; var CloseAction: TCloseAction);
begin
  // 폼이 닫힐 때 처리 - 항상 트레이로 숨기기
  CloseAction := caNone; // 폼이 실제로 닫히지 않도록 함
  Hide; // 대신 폼을 숨김

  // 트레이 아이콘 메뉴 캡션 업데이트
  if Assigned(FTrayIcon) and Assigned(FTrayIcon.PopupMenu) and
     (FTrayIcon.PopupMenu.Items.Count > 0) then
  begin
    FTrayIcon.PopupMenu.Items[0].Caption := '로그 창 보이기';
  end;

  // 소유자 핸들러에게 폼이 숨겨졌음을 알림
  if Assigned(FOwnerHandler) then
    FOwnerHandler.UpdateFormVisibility(False);
end;

procedure TPrintfLogForm.LogTreeGetNodeDataSize(Sender: TBaseVirtualTree;
                                              var NodeDataSize: Integer);
begin
  // 노드 데이터 크기 반환
  NodeDataSize := SizeOf(TLogData);
end;

procedure TPrintfLogForm.LogTreeFreeNode(Sender: TBaseVirtualTree; Node: PVirtualNode);
var
  Data: PLogData;
begin
  // 노드 데이터 해제
  Data := Sender.GetNodeData(Node);
  if Assigned(Data) then
  begin
    Data^.Message := ''; // 문자열 해제
    Data^.Source := '';  // 소스 문자열 해제
  end;
end;

procedure TPrintfLogForm.LogTreeGetText(Sender: TBaseVirtualTree; Node: PVirtualNode;
                                       Column: TColumnIndex; TextType: TVSTTextType;
                                       var CellText: String);
var
  Data: PLogData;
begin
  Data := Sender.GetNodeData(Node);
  if not Assigned(Data) then
    Exit;

  // 저장된 데이터에서 직접 필드 값 사용 - 파싱 재시도 없음
  case Column of
    0: // 시간 컬럼
      begin
        // 시간 형식 수정 - 파일 뷰 모드가 아니고 ShowDateInTimeColumn이 꺼져 있으면 시간만 표시
        if (not FIsFileViewMode) and Assigned(FOwnerHandler) and (not FOwnerHandler.ShowDateInTimeColumn) then
          CellText := FormatDateTime('hh:nn:ss.zzz', Data^.Time)
        else
          CellText := FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Data^.Time);
      end;

    1: // 레벨 컬럼
      begin
        case Data^.Level of
          llDevelop: CellText := 'DEVEL';
          llDebug:   CellText := 'DEBUG';
          llInfo:    CellText := 'INFO';
          llWarning: CellText := 'WARN';
          llError:   CellText := 'ERROR';
          llFatal:   CellText := 'FATAL';
          else       CellText := '???';
        end;
      end;

    2: // 소스 컬럼
      begin
        // 저장된 소스 정보 사용
        CellText := Data^.Source;
      end;

    3: // 메시지 컬럼
      begin
        CellText := Data^.Message;
      end;
  end;
end;


procedure TPrintfLogForm.LogTreeDrawText(Sender: TBaseVirtualTree;
                                       TargetCanvas: TCanvas; Node: PVirtualNode;
                                       Column: TColumnIndex; const CellText: String;
                                       const CellRect: TRect; var DefaultDraw: Boolean);
var
  Data: PLogData;
  SourceColor: TColor;
begin
  Data := Sender.GetNodeData(Node);
  if not Assigned(Data) then
    Exit;

  // 로그 레벨에 따른 텍스트 색상 변경
  if FIsFileViewMode or (Assigned(FOwnerHandler) and FOwnerHandler.ColorByLevel) then
  begin
    case Data^.Level of
      llDevelop:  TargetCanvas.Font.Color := TColor($AAAAFF); // 연한 노란색
      llDebug:    TargetCanvas.Font.Color := TColor($AAFFAA); // 연한 녹색
      llInfo:     TargetCanvas.Font.Color := TColor($FFFFFF); // 흰색
      llWarning:  TargetCanvas.Font.Color := TColor($55AAFF); // 주황색
      llError:    TargetCanvas.Font.Color := TColor($5555FF); // 빨간색
      llFatal:    TargetCanvas.Font.Color := TColor($0000FF); // 진한 빨간색
      else        TargetCanvas.Font.Color := TColor($FFFFFF); // 기본 흰색
    end;
  end;

  // 소스별 색상 적용 (소스 컬럼일 경우)
  if (Column = 2) and Assigned(FOwnerHandler) and FOwnerHandler.ColorBySource then
  begin
    SourceColor := FOwnerHandler.GetColorForSource(Data^.Source);
    if SourceColor <> clNone then
      TargetCanvas.Font.Color := SourceColor;
  end;

  // 기본 그리기 허용
  DefaultDraw := True;
end;

// 로그 트리 헤더 클릭 이벤트 - 소스 컬럼 정렬 및 필터링 팝업
procedure TPrintfLogForm.LogTreeHeaderClick(Sender: TVTHeader; HitInfo: TVTHeaderHitInfo);
var
  Node: PVirtualNode;
  Data: PLogData;
  SourcePopup: TSourcePopupMenu;
  MenuItem: TMenuItem;
  SourceList: TStringList;
  i: Integer;
  SourceStr: string;
  P: TPoint;
begin
  // 헤더 클릭 이벤트 처리 (정렬 등)
  if HitInfo.Button = mbLeft then
  begin
    // 컬럼 정렬 등의 작업 구현 가능
    case HitInfo.Column of
      0: // 시간 컬럼
        begin
          // 시간 정렬 구현
        end;
      1: // 레벨 컬럼
        begin
          // 레벨 정렬 구현
        end;
      2: // 소스 컬럼
        begin
          // 소스 정렬 구현
        end;
      3: // 메시지 컬럼
        begin
          // 메시지 정렬 구현
        end;
    end;
  end
  else if (HitInfo.Button = mbRight) and (HitInfo.Column = 2) then
  begin
    // 소스 컬럼에서 우클릭 시 소스 필터링 팝업 메뉴 표시
    SourceList := TStringList.Create;
    try
      SourceList.Sorted := True;
      SourceList.Duplicates := dupIgnore;

      // 고유 소스 목록
      Node := FLogTree.GetFirst;
      while Assigned(Node) do
      begin
        Data := FLogTree.GetNodeData(Node);
        if Assigned(Data) and (Data^.Source <> '') then
          SourceList.Add(Data^.Source);
        Node := FLogTree.GetNext(Node);
      end;

      if SourceList.Count > 0 then
      begin
        // 소스 필터링 팝업 메뉴 생성
        SourcePopup := TSourcePopupMenu.Create(Self);

        // 메뉴 항목: 모든 소스 표시
        MenuItem := TMenuItem.Create(SourcePopup);
        MenuItem.Caption := '모든 소스 표시';
        MenuItem.OnClick := @ResetFilterButtonClick;
        SourcePopup.Items.Add(MenuItem);

        // 구분선
        MenuItem := TMenuItem.Create(SourcePopup);
        MenuItem.Caption := '-';
        SourcePopup.Items.Add(MenuItem);

        // 각 소스별 필터 메뉴 항목 추가
        for i := 0 to SourceList.Count - 1 do
        begin
          SourceStr := SourceList[i];
          MenuItem := TMenuItem.Create(SourcePopup);
          MenuItem.Caption := SourceStr;
          MenuItem.OnClick := @SourcePopup.FilterSourceClick;
          SourcePopup.Items.Add(MenuItem);
        end;

        // 팝업 메뉴 표시
        P := Mouse.CursorPos;
        SourcePopup.Popup(P.X, P.Y);
      end;
    finally
      SourceList.Free;
    end;
  end;
end;

// 소스 통계 관련 메서드
procedure TPrintfLogForm.AddSourceToList(const SourceStr: string);
begin
  if (SourceStr <> '') then
  begin
    // 이미 목록에 있는지 확인한 후 추가
    if FUniqueSourceList.IndexOf(SourceStr) < 0 then
      FUniqueSourceList.Add(SourceStr);
  end;
end;

procedure TPrintfLogForm.UpdateSourceStatistics;
var
  Node: PVirtualNode;
  Data: PLogData;
  SourceStr: string;
begin
  // 기존 코드 대체

  // 통계 수집을 위해 모든 노드 순회
  FUniqueSourceList.Clear; // 기존 목록 초기화
  FUniqueSourceList.Sorted := True; // 정렬 활성화
  FUniqueSourceList.Duplicates := dupIgnore; // 중복 무시

  Node := FLogTree.GetFirst;
  while Assigned(Node) do
  begin
    Data := FLogTree.GetNodeData(Node);
    if Assigned(Data) then
    begin
      SourceStr := Data^.Source;
      if SourceStr <> '' then
      begin
        // TStringList.Add 메서드 사용 (인덱스 지정 없음)
        if FUniqueSourceList.IndexOf(SourceStr) < 0 then
          FUniqueSourceList.Add(SourceStr);
      end;
    end;
    Node := FLogTree.GetNext(Node);
  end;
end;

procedure TPrintfLogForm.ShowSourceStatistics;
var
  StatForm: TSourceStatForm;
  Node: PVirtualNode;
  Data: PLogData;
  SourceCounts: TStringList; // 소스별 로그 수
  SourceErrorCounts: TStringList; // 소스별 오류/경고 수
  i: Integer;
  SourceStr: string;
  Count, ErrorCount: Integer;
  TempList: TStringList; // 정렬되지 않은 임시 리스트
begin
  // 통계 데이터 수집
  SourceCounts := TStringList.Create;
  SourceErrorCounts := TStringList.Create;
  TempList := TStringList.Create; // 정렬되지 않은 임시 리스트 추가

  try
    // 초기 설정
    SourceCounts.Sorted := False; // 정렬 비활성화
    SourceCounts.Duplicates := dupAccept;
    SourceErrorCounts.Sorted := False; // 정렬 비활성화
    SourceErrorCounts.Duplicates := dupAccept;

    // 먼저 모든 고유 소스 수집 (정렬되지 않은 임시 리스트 사용)
    TempList.Clear;
    TempList.Sorted := False;
    TempList.Duplicates := dupIgnore;

    // 모든 소스 수집
    Node := FLogTree.GetFirst;
    while Assigned(Node) do
    begin
      Data := FLogTree.GetNodeData(Node);
      if Assigned(Data) then
      begin
        SourceStr := Data^.Source;
        if SourceStr = '' then
          SourceStr := '(없음)';

        // 소스가 이미 TempList에 있는지 확인
        if TempList.IndexOf(SourceStr) < 0 then
          TempList.Add(SourceStr);
      end;
      Node := FLogTree.GetNext(Node);
    end;

    // 소스별 카운트 초기화
    for i := 0 to TempList.Count - 1 do
    begin
      SourceCounts.Values[TempList[i]] := '0';
      SourceErrorCounts.Values[TempList[i]] := '0';
    end;

    // 로그 항목 분석
    Node := FLogTree.GetFirst;
    while Assigned(Node) do
    begin
      Data := FLogTree.GetNodeData(Node);
      if Assigned(Data) then
      begin
        SourceStr := Data^.Source;
        if SourceStr = '' then
          SourceStr := '(없음)';

        // 총 카운트 증가
        Count := StrToIntDef(SourceCounts.Values[SourceStr], 0);
        SourceCounts.Values[SourceStr] := IntToStr(Count + 1);

        // 오류/경고 카운트 증가 (ERROR, WARNING, FATAL 레벨)
        if Data^.Level in [llWarning, llError, llFatal] then
        begin
          ErrorCount := StrToIntDef(SourceErrorCounts.Values[SourceStr], 0);
          SourceErrorCounts.Values[SourceStr] := IntToStr(ErrorCount + 1);
        end;
      end;
      Node := FLogTree.GetNext(Node);
    end;

    // 통계 폼 생성 및 표시
    StatForm := TSourceStatForm.Create(Self);
    try
      StatForm.ClearItems;

      // 데이터 추가
      for i := 0 to SourceCounts.Count - 1 do
      begin
        SourceStr := SourceCounts.Names[i];
        Count := StrToIntDef(SourceCounts.Values[SourceStr], 0);
        ErrorCount := StrToIntDef(SourceErrorCounts.Values[SourceStr], 0);

        if Count > 0 then
          StatForm.AddSourceItem(SourceStr, Count, ErrorCount);
      end;

      StatForm.ShowModal;
    finally
      StatForm.Free;
    end;

  finally
    SourceCounts.Free;
    SourceErrorCounts.Free;
    TempList.Free; // 임시 리스트 해제
  end;
end;

procedure TPrintfLogForm.ShowSourceStatisticsForm;
begin
  // 소스 통계 정보 업데이트 및 표시
  UpdateSourceStatistics;
  ShowSourceStatistics;
end;

// 컬럼 가시성 패널 구현
procedure TPrintfLogForm.CreateColumnVisibilityPanel;
begin
  // 컬럼 가시성 패널 생성
  FColumnVisibilityPanel := TPanel.Create(Self);
  with FColumnVisibilityPanel do
  begin
    Parent := Self;
    Align := alTop;
    Height := 80;
    BevelOuter := bvNone;
    BorderStyle := bsSingle;
    Color := clBtnFace;
    Visible := False;
  end;

  // 컬럼 체크박스 생성
  FShowTimeColumnCheck := TCheckBox.Create(FColumnVisibilityPanel);
  with FShowTimeColumnCheck do
  begin
    Parent := FColumnVisibilityPanel;
    Left := 10;
    Top := 10;
    Width := 120;
    Caption := '시간 컬럼 표시';
    Checked := True;
  end;

  FShowLevelColumnCheck := TCheckBox.Create(FColumnVisibilityPanel);
  with FShowLevelColumnCheck do
  begin
    Parent := FColumnVisibilityPanel;
    Left := 140;
    Top := 10;
    Width := 120;
    Caption := '레벨 컬럼 표시';
    Checked := True;
  end;

  FShowSourceColumnCheck := TCheckBox.Create(FColumnVisibilityPanel);
  with FShowSourceColumnCheck do
  begin
    Parent := FColumnVisibilityPanel;
    Left := 10;
    Top := 40;
    Width := 120;
    Caption := '소스 컬럼 표시';
    Checked := True;
  end;

  FShowMessageColumnCheck := TCheckBox.Create(FColumnVisibilityPanel);
  with FShowMessageColumnCheck do
  begin
    Parent := FColumnVisibilityPanel;
    Left := 140;
    Top := 40;
    Width := 150;
    Caption := '메시지 컬럼 표시';
    Checked := True;
  end;

  // 적용 버튼
  FApplyColumnVisibilityButton := TButton.Create(FColumnVisibilityPanel);
  with FApplyColumnVisibilityButton do
  begin
    Parent := FColumnVisibilityPanel;
    Left := FColumnVisibilityPanel.Width - 180;
    Top := 20;
    Width := 80;
    Height := 25;
    Caption := '적용';
    OnClick := @ApplyColumnVisibilityButtonClick;
    Anchors := [akTop, akRight];
  end;

  // 닫기 버튼
  FCloseColumnVisibilityButton := TButton.Create(FColumnVisibilityPanel);
  with FCloseColumnVisibilityButton do
  begin
    Parent := FColumnVisibilityPanel;
    Left := FColumnVisibilityPanel.Width - 90;
    Top := 20;
    Width := 80;
    Height := 25;
    Caption := '닫기';
    OnClick := @CloseColumnVisibilityButtonClick;
    Anchors := [akTop, akRight];
  end;
end;

// 컬럼 가시성 패널 표시/숨기기
procedure TPrintfLogForm.ShowColumnVisibilityPanel;
begin
  // 다른 패널 숨기기
  HideSearchPanel;
  HideFilterPanel;

  // 현재 설정 반영
  FShowTimeColumnCheck.Checked := FColumnVisibility.ShowTimeColumn;
  FShowLevelColumnCheck.Checked := FColumnVisibility.ShowLevelColumn;
  FShowSourceColumnCheck.Checked := FColumnVisibility.ShowSourceColumn;
  FShowMessageColumnCheck.Checked := FColumnVisibility.ShowMessageColumn;

  // 패널 표시
  FColumnVisibilityPanel.Visible := True;
end;

procedure TPrintfLogForm.HideColumnVisibilityPanel;
begin
  FColumnVisibilityPanel.Visible := False;

  // 포커스 트리로 이동
  if FLogTree.CanFocus then
    FLogTree.SetFocus;
end;

// 컬럼 가시성 적용
procedure TPrintfLogForm.ApplyColumnVisibilityButtonClick(Sender: TObject);
begin
  // 설정 업데이트
  FColumnVisibility.ShowTimeColumn := FShowTimeColumnCheck.Checked;
  FColumnVisibility.ShowLevelColumn := FShowLevelColumnCheck.Checked;
  FColumnVisibility.ShowSourceColumn := FShowSourceColumnCheck.Checked;
  FColumnVisibility.ShowMessageColumn := FShowMessageColumnCheck.Checked;

  // 설정 적용
  ApplyColumnVisibility;

  // 상태바 업데이트
  StatusBar.Panels[0].Text := '컬럼 가시성 설정이 적용되었습니다.';
end;

procedure TPrintfLogForm.CloseColumnVisibilityButtonClick(Sender: TObject);
begin
  HideColumnVisibilityPanel;
end;

// 컬럼 가시성 메뉴 버튼 클릭
procedure TPrintfLogForm.ColumnVisibilityButtonClick(Sender: TObject);
begin
  ShowColumnVisibilityPanel;
end;

// 컬럼 가시성 적용 메서드
procedure TPrintfLogForm.ApplyColumnVisibility;
begin
  with FLogTree.Header do
  begin
    // 시간 컬럼 가시성
    if Columns.Count > 0 then
    begin
      if FColumnVisibility.ShowTimeColumn then
        Columns[0].Options := Columns[0].Options + [coVisible]
      else
        Columns[0].Options := Columns[0].Options - [coVisible];
    end;

    // 레벨 컬럼 가시성
    if Columns.Count > 1 then
    begin
      if FColumnVisibility.ShowLevelColumn then
        Columns[1].Options := Columns[1].Options + [coVisible]
      else
        Columns[1].Options := Columns[1].Options - [coVisible];
    end;

    // 소스 컬럼 가시성
    if Columns.Count > 2 then
    begin
      if FColumnVisibility.ShowSourceColumn then
        Columns[2].Options := Columns[2].Options + [coVisible]
      else
        Columns[2].Options := Columns[2].Options - [coVisible];
    end;

    // 메시지 컬럼 가시성
    if Columns.Count > 3 then
    begin
      if FColumnVisibility.ShowMessageColumn then
        Columns[3].Options := Columns[3].Options + [coVisible]
      else
        Columns[3].Options := Columns[3].Options - [coVisible];
    end;
  end;

  // 컬럼 가시성 변경 후 갱신
  FLogTree.Invalidate;

  // 소유 핸들러에 설정 전달 (실시간 로깅 폼인 경우)
  if Assigned(FOwnerHandler) then
    FOwnerHandler.ColumnVisibility := FColumnVisibility;
end;

// 검색 패널 - 소스 검색
procedure TPrintfLogForm.CreateSearchPanel;
begin
  // 검색 패널 생성
  FSearchPanel := TPanel.Create(Self);
  with FSearchPanel do
  begin
    Parent := Self;
    Align := alTop;
    Height := 80;  // 높이 증가
    BevelOuter := bvNone;
    BorderStyle := bsSingle;
    Color := clBtnFace;
    Visible := False;
  end;

  // 검색어 입력
  FSearchEdit := TEdit.Create(FSearchPanel);
  with FSearchEdit do
  begin
    Parent := FSearchPanel;
    Left := 10;
    Top := 10;
    Width := 300;  // 너비 증가
    Anchors := [akLeft, akTop, akRight];
    Text := '';
    TabOrder := 0;
  end;

  // 검색 옵션 체크박스
  FSearchCaseSensitive := TCheckBox.Create(FSearchPanel);
  with FSearchCaseSensitive do
  begin
    Parent := FSearchPanel;
    Left := 10;
    Top := 40;
    Width := 120;
    Caption := '대소문자 구분';
    Checked := False;
  end;

  FSearchWholeWord := TCheckBox.Create(FSearchPanel);
  with FSearchWholeWord do
  begin
    Parent := FSearchPanel;
    Left := 140;
    Top := 40;
    Width := 120;
    Caption := '전체 단어만';
    Checked := False;
  end;

  FSearchInTime := TCheckBox.Create(FSearchPanel);
  with FSearchInTime do
  begin
    Parent := FSearchPanel;
    Left := 270;
    Top := 40;
    Width := 80;
    Caption := '시간';
    Checked := True;
  end;

  FSearchInLevel := TCheckBox.Create(FSearchPanel);
  with FSearchInLevel do
  begin
    Parent := FSearchPanel;
    Left := 350;
    Top := 40;
    Width := 80;
    Caption := '레벨';
    Checked := True;
  end;

  // 소스 검색 체크박스
  FSearchInSource := TCheckBox.Create(FSearchPanel);
  with FSearchInSource do
  begin
    Parent := FSearchPanel;
    Left := 430;
    Top := 40;
    Width := 80;
    Caption := '소스';
    Checked := True;
  end;

  FSearchInMessage := TCheckBox.Create(FSearchPanel);
  with FSearchInMessage do
  begin
    Parent := FSearchPanel;
    Left := 510;
    Top := 40;
    Width := 80;
    Caption := '메시지';
    Checked := True;
  end;

  // 검색 버튼
  FSearchButton := TButton.Create(FSearchPanel);
  with FSearchButton do
  begin
    Parent := FSearchPanel;
    Left := FSearchPanel.Width - 180;
    Top := 10;
    Width := 80;
    Height := 25;
    Caption := '다음 찾기';
    Default := True;
    OnClick := @SearchButtonClick;
    Anchors := [akTop, akRight];
  end;

  // 닫기 버튼
  FCloseSearchButton := TButton.Create(FSearchPanel);
  with FCloseSearchButton do
  begin
    Parent := FSearchPanel;
    Left := FSearchPanel.Width - 90;
    Top := 10;
    Width := 80;
    Height := 25;
    Caption := '닫기';
    OnClick := @CloseSearchButtonClick;
    Anchors := [akTop, akRight];
  end;
end;

// 필터 패널 - 소스 필터링
procedure TPrintfLogForm.CreateFilterPanel;
begin
  // 필터 패널 생성
  FFilterPanel := TPanel.Create(Self);
  with FFilterPanel do
  begin
    Parent := Self;
    Align := alTop;
    Height := 130;  // 높이 증가
    BevelOuter := bvNone;
    BorderStyle := bsSingle;
    Color := clBtnFace;
    Visible := False;
  end;

  // 필터 옵션 - 로그 레벨
  FFilterInfoCheck := TCheckBox.Create(FFilterPanel);
  with FFilterInfoCheck do
  begin
    Parent := FFilterPanel;
    Left := 10;
    Top := 5;
    Width := 60;
    Caption := 'INFO';
    Checked := True;
    OnClick := @FilterCheckChange;
  end;

  FFilterDebugCheck := TCheckBox.Create(FFilterPanel);
  with FFilterDebugCheck do
  begin
    Parent := FFilterPanel;
    Left := 80;
    Top := 5;
    Width := 70;
    Caption := 'DEBUG';
    Checked := True;
    OnClick := @FilterCheckChange;
  end;

  FFilterWarningCheck := TCheckBox.Create(FFilterPanel);
  with FFilterWarningCheck do
  begin
    Parent := FFilterPanel;
    Left := 160;
    Top := 5;
    Width := 60;
    Caption := 'WARN';
    Checked := True;
    OnClick := @FilterCheckChange;
  end;

  FFilterErrorCheck := TCheckBox.Create(FFilterPanel);
  with FFilterErrorCheck do
  begin
    Parent := FFilterPanel;
    Left := 230;
    Top := 5;
    Width := 70;
    Caption := 'ERROR';
    Checked := True;
    OnClick := @FilterCheckChange;
  end;

  FFilterFatalCheck := TCheckBox.Create(FFilterPanel);
  with FFilterFatalCheck do
  begin
    Parent := FFilterPanel;
    Left := 310;
    Top := 5;
    Width := 70;
    Caption := 'FATAL';
    Checked := True;
    OnClick := @FilterCheckChange;
  end;

  FFilterDevelopCheck := TCheckBox.Create(FFilterPanel);
  with FFilterDevelopCheck do
  begin
    Parent := FFilterPanel;
    Left := 390;
    Top := 5;
    Width := 80;
    Caption := 'DEVEL';
    Checked := True;
    OnClick := @FilterCheckChange;
  end;

  // 시간 필터
  FFilterTimeFromCheck := TCheckBox.Create(FFilterPanel);
  with FFilterTimeFromCheck do
  begin
    Parent := FFilterPanel;
    Left := 10;
    Top := 35;
    Width := 60;
    Caption := '시작:';
    Checked := False;
    OnClick := @FilterCheckChange;
  end;

  FFilterTimeFrom := TDateTimePicker.Create(FFilterPanel);
  with FFilterTimeFrom do
  begin
    Parent := FFilterPanel;
    Left := 80;
    Top := 33;
    Width := 150;
    Kind := dtkDateTime;
    Time := StartOfTheDay(Now);
    Enabled := False;
  end;

  FFilterTimeToCheck := TCheckBox.Create(FFilterPanel);
  with FFilterTimeToCheck do
  begin
    Parent := FFilterPanel;
    Left := 240;
    Top := 35;
    Width := 60;
    Caption := '종료:';
    Checked := False;
    OnClick := @FilterCheckChange;
  end;

  FFilterTimeTo := TDateTimePicker.Create(FFilterPanel);
  with FFilterTimeTo do
  begin
    Parent := FFilterPanel;
    Left := 310;
    Top := 33;
    Width := 150;
    Kind := dtkDateTime;
    Time := EndOfTheDay(Now);
    Enabled := False;
  end;

  // 텍스트 필터
  FFilterTextCheck := TCheckBox.Create(FFilterPanel);
  with FFilterTextCheck do
  begin
    Parent := FFilterPanel;
    Left := 10;
    Top := 65;
    Width := 80;
    Caption := '텍스트:';
    Checked := False;
    OnClick := @FilterCheckChange;
  end;

  FFilterText := TEdit.Create(FFilterPanel);
  with FFilterText do
  begin
    Parent := FFilterPanel;
    Left := 100;
    Top := 63;
    Width := 250;
    Enabled := False;
  end;

  FFilterTextCaseSensitive := TCheckBox.Create(FFilterPanel);
  with FFilterTextCaseSensitive do
  begin
    Parent := FFilterPanel;
    Left := 360;
    Top := 65;
    Width := 120;
    Caption := '대소문자 구분';
    Checked := False;
    Enabled := False;
  end;

  // 소스 필터
  FFilterSourceCheck := TCheckBox.Create(FFilterPanel);
  with FFilterSourceCheck do
  begin
    Parent := FFilterPanel;
    Left := 10;
    Top := 95;
    Width := 80;
    Caption := '소스:';
    Checked := False;
    OnClick := @FilterCheckChange;
  end;

  FFilterSource := TEdit.Create(FFilterPanel);
  with FFilterSource do
  begin
    Parent := FFilterPanel;
    Left := 100;
    Top := 93;
    Width := 250;
    Enabled := False;
  end;

  FFilterSourceCaseSensitive := TCheckBox.Create(FFilterPanel);
  with FFilterSourceCaseSensitive do
  begin
    Parent := FFilterPanel;
    Left := 360;
    Top := 95;
    Width := 120;
    Caption := '대소문자 구분';
    Checked := False;
    Enabled := False;
  end;

  // 필터 적용 및 초기화 버튼
  FApplyFilterButton := TButton.Create(FFilterPanel);
  with FApplyFilterButton do
  begin
    Parent := FFilterPanel;
    Left := FFilterPanel.Width - 240;
    Top := 98;//125;
    Width := 80;
    Height := 25;
    Caption := '필터 적용';
    OnClick := @ApplyFilterButtonClick;
    Anchors := [akRight, akBottom];
  end;

  FResetFilterButton := TButton.Create(FFilterPanel);
  with FResetFilterButton do
  begin
    Parent := FFilterPanel;
    Left := FFilterPanel.Width - 150;
    Top := 98;
    Width := 80;
    Height := 25;
    Caption := '필터 초기화';
    OnClick := @ResetFilterButtonClick;
    Anchors := [akRight, akBottom];
  end;

  // 닫기 버튼
  FCloseFilterButton := TButton.Create(FFilterPanel);
  with FCloseFilterButton do
  begin
    Parent := FFilterPanel;
    Left := FFilterPanel.Width - 60;
    Top := 98;
    Width := 50;
    Height := 25;
    Caption := '닫기';
    OnClick := @CloseFilterButtonClick;
    Anchors := [akRight, akBottom];
  end;
end;


// 필터 체크박스 변경 이벤트 - 소스 필터 체크박스
procedure TPrintfLogForm.FilterCheckChange(Sender: TObject);
begin
  if Sender = FFilterTimeFromCheck then
    FFilterTimeFrom.Enabled := FFilterTimeFromCheck.Checked;

  if Sender = FFilterTimeToCheck then
    FFilterTimeTo.Enabled := FFilterTimeToCheck.Checked;

  if Sender = FFilterTextCheck then
  begin
    FFilterText.Enabled := FFilterTextCheck.Checked;
    FFilterTextCaseSensitive.Enabled := FFilterTextCheck.Checked;
  end;

  // 소스 필터 체크박스 처리
  if Sender = FFilterSourceCheck then
  begin
    FFilterSource.Enabled := FFilterSourceCheck.Checked;
    FFilterSourceCaseSensitive.Enabled := FFilterSourceCheck.Checked;
  end;
end;

procedure TPrintfLogForm.ShowSearchPanel;
begin
  // 필터 패널 숨기기
  HideFilterPanel;
  HideColumnVisibilityPanel;

  // 검색 패널 표시
  FSearchPanel.Visible := True;
  FSearchEdit.SetFocus;
end;

procedure TPrintfLogForm.HideSearchPanel;
begin
  FSearchPanel.Visible := False;

  // 포커스 트리로 이동
  if FLogTree.CanFocus then
    FLogTree.SetFocus;
end;

procedure TPrintfLogForm.ShowFilterPanel;
begin
  // 검색 패널 숨기기
  HideSearchPanel;
  HideColumnVisibilityPanel;

  // 필터 패널 표시
  FFilterPanel.Visible := True;
end;

procedure TPrintfLogForm.HideFilterPanel;
begin
  FFilterPanel.Visible := False;

  // 포커스 트리로 이동
  if FLogTree.CanFocus then
    FLogTree.SetFocus;
end;

// 검색 버튼 클릭
procedure TPrintfLogForm.SearchButtonClick(Sender: TObject);
begin
  // 검색 옵션 설정
  FSearchOptions.SearchText := FSearchEdit.Text;
  FSearchOptions.MatchCase := FSearchCaseSensitive.Checked;
  FSearchOptions.WholeWord := FSearchWholeWord.Checked;
  FSearchOptions.SearchInTime := FSearchInTime.Checked;
  FSearchOptions.SearchInLevel := FSearchInLevel.Checked;
  FSearchOptions.SearchInMessage := FSearchInMessage.Checked;
  FSearchOptions.SearchInSource := FSearchInSource.Checked;

  // 다음 찾기 실행
  FindNext;
end;

procedure TPrintfLogForm.CloseSearchButtonClick(Sender: TObject);
begin
  HideSearchPanel;
end;

procedure TPrintfLogForm.FilterButtonClick(Sender: TObject);
begin
  ShowFilterPanel;
end;

// 필터 적용 버튼 클릭
procedure TPrintfLogForm.ApplyFilterButtonClick(Sender: TObject);
begin
  // 필터 옵션 설정
  FFilterOptions.Enabled := True;
  FFilterOptions.ShowInfo := FFilterInfoCheck.Checked;
  FFilterOptions.ShowDebug := FFilterDebugCheck.Checked;
  FFilterOptions.ShowWarning := FFilterWarningCheck.Checked;
  FFilterOptions.ShowError := FFilterErrorCheck.Checked;
  FFilterOptions.ShowFatal := FFilterFatalCheck.Checked;
  FFilterOptions.ShowDevelop := FFilterDevelopCheck.Checked;

  FFilterOptions.FilterTimeEnabled := FFilterTimeFromCheck.Checked or FFilterTimeToCheck.Checked;
  if FFilterTimeFromCheck.Checked then
    FFilterOptions.TimeFrom := FFilterTimeFrom.DateTime;
  if FFilterTimeToCheck.Checked then
    FFilterOptions.TimeTo := FFilterTimeTo.DateTime;

  FFilterOptions.FilterTextEnabled := FFilterTextCheck.Checked;
  if FFilterTextCheck.Checked then
  begin
    FFilterOptions.FilterText := FFilterText.Text;
    FFilterOptions.FilterTextMatchCase := FFilterTextCaseSensitive.Checked;
  end;

  // 소스 필터 설정
  FFilterOptions.FilterSourceEnabled := FFilterSourceCheck.Checked;
  if FFilterSourceCheck.Checked then
  begin
    FFilterOptions.FilterSource := FFilterSource.Text;
    // 대소문자 구분 설정
  end;

  // 필터 적용
  ApplyFilter;

  // 필터 패널 숨기기
  //HideFilterPanel;
end;

// 필터 초기화 버튼 클릭
procedure TPrintfLogForm.ResetFilterButtonClick(Sender: TObject);
begin
  // 체크박스 초기화
  FFilterInfoCheck.Checked := True;
  FFilterDebugCheck.Checked := True;
  FFilterWarningCheck.Checked := True;
  FFilterErrorCheck.Checked := True;
  FFilterFatalCheck.Checked := True;
  FFilterDevelopCheck.Checked := True;

  FFilterTimeFromCheck.Checked := False;
  FFilterTimeToCheck.Checked := False;
  FFilterTextCheck.Checked := False;
  FFilterSourceCheck.Checked := False;

  FilterCheckChange(FFilterTimeFromCheck);
  FilterCheckChange(FFilterTimeToCheck);
  FilterCheckChange(FFilterTextCheck);
  FilterCheckChange(FFilterSourceCheck);

  // 필터 옵션 초기화
  FFilterOptions.Enabled := False;

  // 원래 노드 복원
  RestoreNodes;

  // 필터 패널 숨기기
  //HideFilterPanel;

  // 상태바 업데이트
  StatusBar.Panels[0].Text := Format('모든 필터 초기화됨 (%d 항목)', [FLogTree.TotalCount]);
end;

procedure TPrintfLogForm.CloseFilterButtonClick(Sender: TObject);
begin
  HideFilterPanel;
end;

// 다음 찾기
procedure TPrintfLogForm.FindNext;
var
  StartNode: PVirtualNode;
  FoundNode: PVirtualNode;
begin
  if FSearchOptions.SearchText = '' then
    Exit;

  // 시작 노드 결정
  if FCurrentSearchNode = nil then
    StartNode := FLogTree.GetFirst
  else
    StartNode := FLogTree.GetNext(FCurrentSearchNode);

  if StartNode = nil then
    StartNode := FLogTree.GetFirst;

  // 다음 일치 항목 찾기
  FoundNode := FindNextMatch(StartNode, FSearchOptions);

  if FoundNode <> nil then
  begin
    // 일치 항목 발견
    FCurrentSearchNode := FoundNode;
    HighlightSearchMatch(FoundNode);
    StatusBar.Panels[0].Text := Format('"%s" 찾음', [FSearchOptions.SearchText]);
  end
  else
  begin
    // 일치 항목 없음
    if FCurrentSearchNode <> nil then
    begin
      // 처음부터 다시 검색
      StartNode := FLogTree.GetFirst;
      FoundNode := FindNextMatch(StartNode, FSearchOptions);

      if FoundNode <> nil then
      begin
        FCurrentSearchNode := FoundNode;
        HighlightSearchMatch(FoundNode);
        StatusBar.Panels[0].Text := Format('처음부터 "%s" 찾음', [FSearchOptions.SearchText]);
      end
      else
      begin
        StatusBar.Panels[0].Text := Format('"%s" 찾을 수 없음', [FSearchOptions.SearchText]);
      end;
    end
    else
    begin
      StatusBar.Panels[0].Text := Format('"%s" 찾을 수 없음', [FSearchOptions.SearchText]);
    end;
  end;
end;

// 이전 찾기
procedure TPrintfLogForm.FindPrev;
var
  StartNode, Node, PrevNode, FoundNode: PVirtualNode;
  SearchStarted: Boolean;
begin
  if FSearchOptions.SearchText = '' then
    Exit;

  // 시작 노드 결정
  if FCurrentSearchNode = nil then
  begin
    StartNode := FLogTree.GetLast;
    SearchStarted := False;
  end
  else
  begin
    StartNode := FCurrentSearchNode;
    SearchStarted := True;
  end;

  if StartNode = nil then
    Exit;

  // 일치 항목 찾기 (역방향 검색은 느리므로 마지막 노드부터 시작해서 일치하면 중단)
  FoundNode := nil;
  PrevNode := nil;
  Node := FLogTree.GetFirst;

  while Assigned(Node) do
  begin
    // 현재 검색 노드 이전까지만 검색
    if SearchStarted and (Node = FCurrentSearchNode) then
      Break;

    // 일치 여부 확인
    if FindNextMatch(Node, FSearchOptions) = Node then
      PrevNode := Node;

    Node := FLogTree.GetNext(Node);
  end;

  // 이전 결과가 있으면 표시
  if PrevNode <> nil then
  begin
    FCurrentSearchNode := PrevNode;
    HighlightSearchMatch(PrevNode);
    StatusBar.Panels[0].Text := Format('"%s" 찾음', [FSearchOptions.SearchText]);
  end
  else
  begin
    StatusBar.Panels[0].Text := Format('"%s" 찾을 수 없음', [FSearchOptions.SearchText]);
  end;
end;

// 다음 일치 항목 찾기
function TPrintfLogForm.FindNextMatch(StartNode: PVirtualNode; SearchOptions: TSearchOptions): PVirtualNode;
var
  Node: PVirtualNode;
  Data: PLogData;
  TimeStr, LevelStr, MsgStr, SourceStr: string;
  IsMatched: Boolean;
  SearchText: string;
begin
  Result := nil;

  // 검색어가 비어있으면 종료
  if SearchOptions.SearchText = '' then
    Exit;

  // 검색어 준비
  if SearchOptions.MatchCase then
    SearchText := SearchOptions.SearchText
  else
    SearchText := LowerCase(SearchOptions.SearchText);

  // 시작 노드부터 순회
  Node := StartNode;
  while Assigned(Node) do
  begin
    Data := FLogTree.GetNodeData(Node);
    if Assigned(Data) then
    begin
      IsMatched := False;

      // 시간 컬럼 검색
      if SearchOptions.SearchInTime then
      begin
        TimeStr := FLogTree.Text[Node, 0];
        if not SearchOptions.MatchCase then
          TimeStr := LowerCase(TimeStr);

        if SearchOptions.WholeWord then
          IsMatched := SameText(TimeStr, SearchText)
        else
          IsMatched := Pos(SearchText, TimeStr) > 0;
      end;

      // 레벨 컬럼 검색
      if (not IsMatched) and SearchOptions.SearchInLevel then
      begin
        LevelStr := FLogTree.Text[Node, 1];
        if not SearchOptions.MatchCase then
          LevelStr := LowerCase(LevelStr);

        if SearchOptions.WholeWord then
          IsMatched := SameText(LevelStr, SearchText)
        else
          IsMatched := Pos(SearchText, LevelStr) > 0;
      end;

      // 소스 컬럼 검색
      if (not IsMatched) and SearchOptions.SearchInSource then
      begin
        SourceStr := FLogTree.Text[Node, 2]; // 소스 컬럼
        if not SearchOptions.MatchCase then
          SourceStr := LowerCase(SourceStr);

        if SearchOptions.WholeWord then
          IsMatched := SameText(SourceStr, SearchText)
        else
          IsMatched := Pos(SearchText, SourceStr) > 0;
      end;

      // 메시지 컬럼 검색
      if (not IsMatched) and SearchOptions.SearchInMessage then
      begin
        MsgStr := FLogTree.Text[Node, 3]; // 메시지 컬럼
        if not SearchOptions.MatchCase then
          MsgStr := LowerCase(MsgStr);

        if SearchOptions.WholeWord then
          IsMatched := SameText(MsgStr, SearchText)
        else
          IsMatched := Pos(SearchText, MsgStr) > 0;
      end;

      // 일치 항목 발견
      if IsMatched then
      begin
        Result := Node;
        Exit;
      end;
    end;

    // 다음 노드로 이동
    Node := FLogTree.GetNext(Node);
  end;
end;

procedure TPrintfLogForm.HighlightSearchMatch(Node: PVirtualNode);
begin
  // 노드 선택 및 표시
  FLogTree.ClearSelection;
  FLogTree.Selected[Node] := True;
  FLogTree.FocusedNode := Node;
  FLogTree.ScrollIntoView(Node, True);
end;

// 원본 노드 저장
procedure TPrintfLogForm.StoreOriginalNodes;
var
  Node: PVirtualNode;
begin
  // 기존 리스트 해제
  if Assigned(FOriginalNodes) then
    FOriginalNodes.Free;

  // 새 리스트 생성
  FOriginalNodes := TList.Create;

  // 모든 노드 저장
  Node := FLogTree.GetFirst;
  while Assigned(Node) do
  begin
    FOriginalNodes.Add(Node);
    Node := FLogTree.GetNext(Node);
  end;
end;


// 원본 노드 복원
procedure TPrintfLogForm.RestoreNodes;
var
  i: Integer;
  Node: PVirtualNode;
begin
  // 현재 모든 노드 숨기기
  Node := FLogTree.GetFirst;
  while Assigned(Node) do
  begin
    FLogTree.IsVisible[Node] := False;
    Node := FLogTree.GetNext(Node);
  end;

  // 원본 노드 표시
  if Assigned(FOriginalNodes) then
  begin
    for i := 0 to FOriginalNodes.Count - 1 do
    begin
      Node := PVirtualNode(FOriginalNodes[i]);
      FLogTree.IsVisible[Node] := True;
    end;
  end;

  // 트리 갱신
  FLogTree.Invalidate;
end;

// 필터 조건 매칭 메서드
function TPrintfLogForm.MatchesFilter(Node: PVirtualNode): Boolean;
var
  Data: PLogData;
  LevelStr, MsgStr, SourceStr: string; // 소스 문자열 추가
  IsLevelMatched, IsTimeMatched, IsTextMatched, IsSourceMatched: Boolean; // 소스 매칭
begin
  Result := False;
  Data := FLogTree.GetNodeData(Node);
  if not Assigned(Data) then
    Exit;

  // 레벨 필터 확인
  LevelStr := FLogTree.Text[Node, 1];
  IsLevelMatched := False;

  if (LevelStr = 'INFO') and FFilterOptions.ShowInfo then
    IsLevelMatched := True
  else if (LevelStr = 'DEBUG') and FFilterOptions.ShowDebug then
    IsLevelMatched := True
  else if ((LevelStr = 'WARN') or (LevelStr = 'WARNING')) and FFilterOptions.ShowWarning then
    IsLevelMatched := True
  else if (LevelStr = 'ERROR') and FFilterOptions.ShowError then
    IsLevelMatched := True
  else if (LevelStr = 'FATAL') and FFilterOptions.ShowFatal then
    IsLevelMatched := True
  else if (LevelStr = 'DEVEL') and FFilterOptions.ShowDevelop then
    IsLevelMatched := True;

  if not IsLevelMatched then
    Exit;

  // 시간 필터 확인
  IsTimeMatched := True;
  if FFilterOptions.FilterTimeEnabled then
  begin
    if FFilterTimeFromCheck.Checked and (Data^.Time < FFilterOptions.TimeFrom) then
      IsTimeMatched := False;

    if FFilterTimeToCheck.Checked and (Data^.Time > FFilterOptions.TimeTo) then
      IsTimeMatched := False;
  end;

  if not IsTimeMatched then
    Exit;

  // 텍스트 필터 확인
  IsTextMatched := True;
  if FFilterOptions.FilterTextEnabled and (FFilterOptions.FilterText <> '') then
  begin
    MsgStr := FLogTree.Text[Node, 3];

    if not FFilterOptions.FilterTextMatchCase then
    begin
      MsgStr := LowerCase(MsgStr);
      IsTextMatched := Pos(LowerCase(FFilterOptions.FilterText), MsgStr) > 0;
    end
    else
      IsTextMatched := Pos(FFilterOptions.FilterText, MsgStr) > 0;
  end;

  if not IsTextMatched then
    Exit;

  // 소스 필터 확인
  IsSourceMatched := True;
  if FFilterOptions.FilterSourceEnabled and (FFilterOptions.FilterSource <> '') then
  begin
    SourceStr := FLogTree.Text[Node, 2]; // 소스 컬럼

    if not FFilterOptions.FilterTextMatchCase then // 대소문자 구분 설정은 텍스트와 동일하게 사용
    begin
      SourceStr := LowerCase(SourceStr);
      IsSourceMatched := Pos(LowerCase(FFilterOptions.FilterSource), SourceStr) > 0;
    end
    else
      IsSourceMatched := Pos(FFilterOptions.FilterSource, SourceStr) > 0;
  end;

  Result := IsLevelMatched and IsTimeMatched and IsTextMatched and IsSourceMatched;
end;

// 필터 적용
procedure TPrintfLogForm.ApplyFilter;
var
  Node: PVirtualNode;
  VisibleCount: Integer;
begin
  // 필터가 활성화되지 않았으면 모든 노드 표시
  if not FFilterOptions.Enabled then
  begin
    RestoreNodes;
    Exit;
  end;

  // 모든 노드 순회하며 필터 조건 확인
  VisibleCount := 0;
  Node := FLogTree.GetFirst;
  while Assigned(Node) do
  begin
    if MatchesFilter(Node) then
    begin
      FLogTree.IsVisible[Node] := True;
      Inc(VisibleCount);
    end
    else
      FLogTree.IsVisible[Node] := False;

    Node := FLogTree.GetNext(Node);
  end;

  // 트리 갱신
  FLogTree.Invalidate;

  // 상태바 업데이트
  StatusBar.Panels[0].Text := Format('필터 적용됨: %d/%d 항목 표시', [VisibleCount, FOriginalNodes.Count]);
end;

procedure TPrintfLogForm.CreatePopupMenu;
var
  PopupMenu1: TPopupMenu;
  MenuItem: TMenuItem;
begin
  PopupMenu1 := TPopupMenu.Create(Self);

  // 로그 보이기 메뉴 (처음에는 비활성화)
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '로그 보이기';
  MenuItem.Tag := 4; // 로그 보이기 태그
  MenuItem.OnClick := @PopupMenuClick;
  MenuItem.Enabled := False; // 처음에는 비활성화 (폼이 보이는 상태이므로)
  PopupMenu1.Items.Add(MenuItem);

  // 구분선
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '-';
  PopupMenu1.Items.Add(MenuItem);

  // 복사 메뉴
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '선택한 로그 복사 (Ctrl+C)';
  MenuItem.Tag := 3; // 복사 태그
  MenuItem.OnClick := @PopupMenuClick;
  PopupMenu1.Items.Add(MenuItem);

  // 구분선
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '-';
  PopupMenu1.Items.Add(MenuItem);

  // 클리어 메뉴
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '로그 지우기 (Ctrl+L)';
  MenuItem.Tag := 1;
  MenuItem.OnClick := @PopupMenuClick;
  PopupMenu1.Items.Add(MenuItem);

  // 구분선
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '-';
  PopupMenu1.Items.Add(MenuItem);

  // 저장 메뉴
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '로그 파일 저장... (Ctrl+S)';
  MenuItem.Tag := 2;
  MenuItem.OnClick := @PopupMenuClick;
  PopupMenu1.Items.Add(MenuItem);

  // 구분선
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '-';
  PopupMenu1.Items.Add(MenuItem);

  // 컬럼 가시성 메뉴
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '컬럼 표시/숨김... (Ctrl+V)';
  MenuItem.Tag := 5;
  MenuItem.OnClick := @PopupMenuClick;
  PopupMenu1.Items.Add(MenuItem);

  // 소스 통계 메뉴
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '소스별 통계 보기... (Ctrl+G)';
  MenuItem.Tag := 6;
  MenuItem.OnClick := @PopupMenuClick;
  PopupMenu1.Items.Add(MenuItem);

  // 폼에 팝업 메뉴 설정
  FLogTree.PopupMenu := PopupMenu1;
end;

// 팝업 메뉴 클릭 이벤트 - 소스 통계 및 컬럼 가시성 메뉴
procedure TPrintfLogForm.PopupMenuClick(Sender: TObject);
begin
  case TMenuItem(Sender).Tag of
    1: // 클리어
      begin
        ClearLogs;
        if Assigned(FOnClear) then
          FOnClear(Self);
      end;
    2: // 저장
      begin
        SaveLogs;
        if Assigned(FOnSave) then
          FOnSave(Self);
      end;
    3: // 복사
      begin
        CopySelectedLogs;
      end;
    4: // 로그 보이기 또는 종료 (트레이 아이콘 메뉴에서 호출 시)
      begin
        if TMenuItem(Sender).Caption = '로그 창 종료' then
        begin
          // 실제 종료 처리
          if Assigned(FOwnerHandler) then
            FOwnerHandler.Shutdown;
        end
        else if Assigned(FOwnerHandler) then
        begin
          FOwnerHandler.ShowForm(True);
          // '로그 보이기' 메뉴 항목 비활성화 (폼이 이제 보이므로)
          if Sender is TMenuItem then
            TMenuItem(Sender).Enabled := False;
        end;
      end;
    5: // 컬럼 가시성 설정
      begin
        ShowColumnVisibilityPanel;
      end;
    6: // 소스 통계 보기
      begin
        ShowSourceStatisticsForm;
      end;
  end;
end;

procedure TPrintfLogForm.FormKeyDown(Sender: TObject; var Key: Word; Shift: TShiftState);
begin
  // Ctrl+S: 저장
  if (Key = Ord('S')) and (ssCtrl in Shift) then
  begin
    SaveLogs;
    if Assigned(FOnSave) then
      FOnSave(Self);
    Key := 0;
  end
  // Ctrl+L: 로그 지우기
  else if (Key = Ord('L')) and (ssCtrl in Shift) then
  begin
    ClearLogs;
    if Assigned(FOnClear) then
      FOnClear(Self);
    Key := 0;
  end
  // Ctrl+C: 복사
  else if (Key = Ord('C')) and (ssCtrl in Shift) then
  begin
    CopySelectedLogs;
    Key := 0;
  end
  // Ctrl+V: 컬럼 가시성 패널 표시
  else if (Key = Ord('V')) and (ssCtrl in Shift) then
  begin
    ShowColumnVisibilityPanel;
    Key := 0;
  end;

  // Ctrl+F: 검색 패널 표시
  if (Key = Ord('F')) and (ssCtrl in Shift) then
  begin
    ShowSearchPanel;
    Key := 0;
  end
  // F3: 다음 찾기
  else if Key = VK_F3 then
  begin
    FindNext;
    Key := 0;
  end
  // Shift+F3: 이전 찾기
  else if (Key = VK_F3) and (ssShift in Shift) then
  begin
    FindPrev;
    Key := 0;
  end;

  // Ctrl+T: 필터 패널 표시
  if (Key = Ord('T')) and (ssCtrl in Shift) then
  begin
    ShowFilterPanel;
    Key := 0;
  end;

  // Ctrl+G: 소스 통계 표시
  if (Key = Ord('G')) and (ssCtrl in Shift) then
  begin
    ShowSourceStatisticsForm;
    Key := 0;
  end;

  // F5: 새로고침
  if Key = VK_F5 then
  begin
    if FIsFileViewMode and (FLastLoadedFileName <> '') then
      ReloadLogFile;
    Key := 0;
  end;
end;

// 선택한 로그 복사 메서드 - 소스 정보 포함
procedure TPrintfLogForm.CopySelectedLogs;
var
  SelectedText: string;
  Node: PVirtualNode;
  TimeStr, LevelStr, SourceStr, MsgStr: string;
  LineText: string;
begin
  SelectedText := '';

  // 선택된 항목이 없으면 현재 항목 사용
  if FLogTree.SelectedCount = 0 then
  begin
    Node := FLogTree.FocusedNode;
    if Assigned(Node) then
    begin
      // 각 컬럼의 텍스트 가져오기
      TimeStr := FLogTree.Text[Node, 0];
      LevelStr := FLogTree.Text[Node, 1];
      SourceStr := FLogTree.Text[Node, 2]; // 소스 컬럼
      MsgStr := FLogTree.Text[Node, 3];     // 메시지 컬럼

      // 시간 + 레벨 + 소스 + 메시지 형식으로 조합
      if SourceStr <> '' then
        LineText := Format('[%s] [%s] [%s] %s', [TimeStr, LevelStr, SourceStr, MsgStr])
      else
        LineText := Format('[%s] [%s] %s', [TimeStr, LevelStr, MsgStr]);

      SelectedText := LineText;
    end;
  end
  else
  begin
    // 선택된 모든 항목 수집
    Node := FLogTree.GetFirstSelected;
    while Assigned(Node) do
    begin
      // 각 컬럼의 텍스트 가져오기
      TimeStr := FLogTree.Text[Node, 0];
      LevelStr := FLogTree.Text[Node, 1];
      SourceStr := FLogTree.Text[Node, 2]; // 소스 컬럼
      MsgStr := FLogTree.Text[Node, 3];     // 메시지 컬럼

      // 시간 + 레벨 + 소스 + 메시지 형식으로 조합
      if SourceStr <> '' then
        LineText := Format('[%s] [%s] [%s] %s', [TimeStr, LevelStr, SourceStr, MsgStr])
      else
        LineText := Format('[%s] [%s] %s', [TimeStr, LevelStr, MsgStr]);

      if SelectedText <> '' then
        SelectedText := SelectedText + LineEnding;
      SelectedText := SelectedText + LineText;

      Node := FLogTree.GetNextSelected(Node);
    end;
  end;

  // 클립보드에 복사
  if SelectedText <> '' then
  begin
    Clipboard.AsText := SelectedText;
    FStatusBar.Panels[0].Text := '선택된 로그가 클립보드에 복사되었습니다.';
  end;
end;


procedure TPrintfLogForm.StatusBarDblClick(Sender: TObject);
begin
  // 추적 모드 토글
  if FTraceMode = tmNormal then
  begin
    FTraceMode := tmAutoScroll;
    FStatusBar.Panels[1].Text := '더블클릭: 자동 스크롤 비활성화';
  end
  else
  begin
    FTraceMode := tmNormal;
    FStatusBar.Panels[1].Text := '더블클릭: 자동 스크롤 활성화';
  end;
end;

// 로그 항목
procedure TPrintfLogForm.AddLog(const LogText: string; Level: TLogLevel = llInfo; const Source: string = '');
var
  Node: PVirtualNode;
  Data: PLogData;
begin
  // 노드
  Node := FLogTree.AddChild(nil);
  Data := FLogTree.GetNodeData(Node);

  if Assigned(Data) then
  begin
    Data^.Message := LogText;
    Data^.Level := Level;
    Data^.Time := Now;
    Data^.Source := Source; // 소스 정보 설정

    // 고유 소스
    if Source <> '' then
      AddSourceToList(Source);
  end;

  // 폼이 보이는 경우에만 UI 업데이트 작업 수행
  if Visible then
  begin
    // 자동 스크롤
    if FTraceMode = tmAutoScroll then
      FLogTree.ScrollIntoView(Node, True);

    // UI 업데이트
    Application.ProcessMessages;
  end;

  // 로그 로테이션 확인 - 폼 가시성과 무관하게 수행
  if Assigned(FOwnerHandler) and
     FOwnerHandler.AutoRotate and
     (FLogTree.TotalCount >= FOwnerHandler.RotateLineCount) then
  begin
    if FOwnerHandler.RotateSaveBeforeClear then
    begin
      if not DirectoryExists(FOwnerHandler.RotateLogFolder) then
        ForceDirectories(FOwnerHandler.RotateLogFolder);

      SaveLogs(FOwnerHandler.RotateLogFolder + PathDelim +
               FormatDateTime('yyyymmdd-hhnnss', Now) + '.log');
    end;

    ClearLogs;
  end;
end;

// 로그 저장
procedure TPrintfLogForm.SaveLogs(const FileName: string = '');
var
  SaveDialog: TSaveDialog;
  ActualFileName: string;
  LogFile: TextFile;
  FileHandle: THandle;
  Node: PVirtualNode;
  Data: PLogData;
  TimeStr, LevelStr, SourceStr, MsgStr: string;
  FilterActive: Boolean;
  FileStream: TFileStream;
  TempStream: TMemoryStream;
begin
  // 파일 이름 결정
  if FileName <> '' then
    ActualFileName := FileName
  else
  begin
    SaveDialog := TSaveDialog.Create(Self);
    try
      SaveDialog.Title := '로그 파일 저장';
      SaveDialog.DefaultExt := '.log';
      SaveDialog.Filter := '로그 파일 (*.log)|*.log|텍스트 파일 (*.txt)|*.txt|모든 파일 (*.*)|*.*';
      SaveDialog.InitialDir := ExtractFilePath(Application.ExeName);
      SaveDialog.FileName := FormatDateTime('yyyymmdd-hhnnss', Now) + '.log';

      if SaveDialog.Execute then
        ActualFileName := SaveDialog.FileName
      else
        Exit;
    finally
      SaveDialog.Free;
    end;
  end;

  // 필터 적용 여부 확인
  FilterActive := FIsFileViewMode and FFilterOptions.Enabled;

  // 파일 저장 시도
  try
    try
      // 공유 모드로 파일 생성 시도
      FileHandle := FileCreate(ActualFileName);
      if FileHandle > 0 then
      begin
        FileClose(FileHandle);

        // 파일 열기
        AssignFile(LogFile, ActualFileName);
        Rewrite(LogFile);

        try
          // 모든 로그 항목 저장
          Node := FLogTree.GetFirst;
          while Assigned(Node) do
          begin
            // 필터가 활성화된 경우 보이는 노드만 저장
            if (not FilterActive) or FLogTree.IsVisible[Node] then
            begin
              Data := FLogTree.GetNodeData(Node);
              if Assigned(Data) then
              begin
                // 시간, 레벨, 소스, 메시지 가져오기
                TimeStr := FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Data^.Time);

                case Data^.Level of
                  llDevelop: LevelStr := 'DEVEL';
                  llDebug:   LevelStr := 'DEBUG';
                  llInfo:    LevelStr := 'INFO';
                  llWarning: LevelStr := 'WARN';
                  llError:   LevelStr := 'ERROR';
                  llFatal:   LevelStr := 'FATAL';
                  else       LevelStr := '???';
                end;

                SourceStr := Data^.Source;
                MsgStr := Data^.Message;

                // 파일에 쓰기 - 소스 정보 포함
                if SourceStr <> '' then
                  WriteLn(LogFile, Format('[%s] [%s] [%s] %s', [TimeStr, LevelStr, SourceStr, MsgStr]))
                else
                  WriteLn(LogFile, Format('[%s] [%s] %s', [TimeStr, LevelStr, MsgStr]));
              end;
            end;

            Node := FLogTree.GetNextSibling(Node);
          end;

          // 저장 성공 메시지
          if FilterActive then
            StatusBar.Panels[0].Text := Format('필터링된 로그 저장 완료: %s', [ExtractFileName(ActualFileName)])
          else
            StatusBar.Panels[0].Text := Format('파일 저장 완료: %s', [ExtractFileName(ActualFileName)]);
        finally
          CloseFile(LogFile);
        end;
      end;
    except
      on E: Exception do
      begin
        // 일반적인 방법으로 저장 실패 시 대체 방법 시도
        try
          // TStringList로 저장 시도
          with TStringList.Create do
          try
            Node := FLogTree.GetFirst;
            while Assigned(Node) do
            begin
              // 필터가 활성화된 경우 보이는 노드만 저장
              if (not FilterActive) or FLogTree.IsVisible[Node] then
              begin
                Data := FLogTree.GetNodeData(Node);
                if Assigned(Data) then
                begin
                  // 시간, 레벨, 소스, 메시지 가져오기
                  TimeStr := FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Data^.Time);

                  case Data^.Level of
                    llDevelop: LevelStr := 'DEVEL';
                    llDebug:   LevelStr := 'DEBUG';
                    llInfo:    LevelStr := 'INFO';
                    llWarning: LevelStr := 'WARN';
                    llError:   LevelStr := 'ERROR';
                    llFatal:   LevelStr := 'FATAL';
                    else       LevelStr := '???';
                  end;

                  SourceStr := Data^.Source;
                  MsgStr := Data^.Message;

                  // 파일에 쓰기 - 소스 정보 포함
                  if SourceStr <> '' then
                    Add(Format('[%s] [%s] [%s] %s', [TimeStr, LevelStr, SourceStr, MsgStr]))
                  else
                    Add(Format('[%s] [%s] %s', [TimeStr, LevelStr, MsgStr]));
                end;
              end;

              Node := FLogTree.GetNextSibling(Node);
            end;

            // 파일 저장
            SaveToFile(ActualFileName);

            StatusBar.Panels[0].Text := Format('파일 저장 완료(대체 방법): %s', [ExtractFileName(ActualFileName)]);
          finally
            Free;
          end;
        except
          on E2: Exception do
            // 저장 실패 메시지
            StatusBar.Panels[0].Text := '파일 저장 실패: ' + E2.Message;
        end;
      end;
    end;
  except
    on E: Exception do
      // 저장 실패 메시지
      StatusBar.Panels[0].Text := '파일 저장 실패: ' + E.Message;
  end;
end;

// 로그 클리어
procedure TPrintfLogForm.ClearLogs;
begin
  // VirtualTreeView 초기화
  FLogTree.Clear;
  FStatusBar.Panels[0].Text := '로그 지워짐: ' + FormatDateTime('hh:nn:ss', Now);
end;

// 로그 파일 로드 메서드
procedure TPrintfLogForm.LoadLogsFromFile(const FileName: string);
var
  LogFile: TextFile;
  Line: string;
  TimeStr, LevelStr, MsgStr, SourceStr: string;
  Node: PVirtualNode;
  Data: PLogData;
  TimeStart, TimeEnd, LevelStart, LevelEnd, SourceStart, SourceEnd, MsgStart: Integer;
  IsValidTimeFormat, IsValidLevelFormat: Boolean;
  TempFileName: string;
  FileStream: TFileStream;
  TempStream: TMemoryStream;
  FileHandle: THandle;
begin
  // 기존 로그 지우기
  FLogTree.Clear;

  // 고유 소스 목록 초기화
  FUniqueSourceList.Clear;

  if not FileExists(FileName) then
  begin
    ShowMessage('로그 파일을 찾을 수 없습니다: ' + FileName);
    Exit;
  end;

  try
    // 먼저 공유 모드로 파일을 열기 시도
    try
      // 공유 읽기 모드로 파일 열기 시도
      FileStream := TFileStream.Create(FileName, fmOpenRead or fmShareDenyNone);
      try
        // 파일 핸들 획득 성공, 일반적인 방법으로 처리 진행
        AssignFile(LogFile, FileName);
        Reset(LogFile);

        // 파일의 각 라인 처리
        while not Eof(LogFile) do
        begin
          ReadLn(LogFile, Line);

          // 로그 라인 파싱
          TimeStr := '';
          LevelStr := '';
          SourceStr := '';
          MsgStr := Line;  // 기본값은 전체 라인
          IsValidTimeFormat := False;
          IsValidLevelFormat := False;

          // [시간] [레벨] [소스] 메시지 형식 파싱
          TimeStart := Pos('[', Line);
          if TimeStart > 0 then
          begin
            TimeEnd := Pos(']', Line, TimeStart);
            if TimeEnd > TimeStart then
            begin
              TimeStr := Copy(Line, TimeStart + 1, TimeEnd - TimeStart - 1);

              // 시간 형식 검증 - 날짜/시간 형식인지 확인
              IsValidTimeFormat := IsValidDateTimeFormat(TimeStr);

              if IsValidTimeFormat then  // 유효한 시간 형식일 경우에만 계속 파싱
              begin
                // 레벨 정보 파싱
                LevelStart := Pos('[', Line, TimeEnd);
                if LevelStart > 0 then
                begin
                  LevelEnd := Pos(']', Line, LevelStart);
                  if LevelEnd > LevelStart then
                  begin
                    LevelStr := Copy(Line, LevelStart + 1, LevelEnd - LevelStart - 1);

                    // 레벨 형식 검증 - 알려진 로그 레벨인지 확인
                    IsValidLevelFormat := IsValidLogLevel(LevelStr);

                    if IsValidLevelFormat then  // 유효한 레벨 형식일 경우에만 계속 파싱
                    begin
                      // 소스 정보 파싱 (있을 경우)
                      SourceStart := Pos('[', Line, LevelEnd);
                      if SourceStart > 0 then
                      begin
                        SourceEnd := Pos(']', Line, SourceStart);
                        if SourceEnd > SourceStart then
                        begin
                          SourceStr := Copy(Line, SourceStart + 1, SourceEnd - SourceStart - 1);
                          MsgStart := SourceEnd + 1;
                        end
                        else
                          MsgStart := LevelEnd + 1;
                      end
                      else
                        MsgStart := LevelEnd + 1;

                      // 메시지 부분 파싱
                      if MsgStart <= Length(Line) then
                        MsgStr := Trim(Copy(Line, MsgStart, Length(Line)));
                    end
                    else
                    begin
                      // 레벨 형식이 아니면 원본 라인 사용
                      LevelStr := '';
                      MsgStr := Line;
                    end;
                  end;
                end;
              end
              else
              begin
                // 시간 형식이 아니면 원본 라인 사용
                TimeStr := '';
                MsgStr := Line;
              end;
            end;
          end;

          // 노드 추가
          Node := FLogTree.AddChild(nil);
          Data := FLogTree.GetNodeData(Node);
          if Assigned(Data) then
          begin
            // 파싱에 성공한 경우
            if IsValidTimeFormat and IsValidLevelFormat then
            begin
              Data^.Message := MsgStr;  // 메시지 부분 저장
              Data^.Source := SourceStr; // 소스 정보 저장

              // 소스가 있으면 고유 소스 목록에 추가
              if SourceStr <> '' then
                AddSourceToList(SourceStr);

              // 레벨 설정
              Data^.Level := StringToLogLevel(LevelStr);

              // 시간 설정
              try
                Data^.Time := StrToDateTime(TimeStr);
              except
                Data^.Time := Now;
              end;
            end
            else
            begin
              // 파싱 실패한 경우 - 기본 구분자 기반 파싱 시도
              if not ParseLogLineBasic(Line, TimeStr, LevelStr, SourceStr, MsgStr) then
              begin
                // 파싱 실패 - 전체 라인을 메시지로 처리
                Data^.Message := Line;
                Data^.Source := '';
                Data^.Level := llInfo; // 기본값
                Data^.Time := Now;
              end
              else
              begin
                // 기본 파싱 성공
                Data^.Message := MsgStr;
                Data^.Source := SourceStr;
                if SourceStr <> '' then
                  AddSourceToList(SourceStr);
                Data^.Level := StringToLogLevel(LevelStr);
                try
                  if TimeStr <> '' then
                    Data^.Time := StrToDateTime(TimeStr)
                  else
                    Data^.Time := Now;
                except
                  Data^.Time := Now;
                end;
              end;
            end;
          end;
        end;

        // 파일 닫기
        CloseFile(LogFile);
      finally
        FileStream.Free;
      end;
    except
      on E: Exception do
      begin
        // 공유 모드로 열기 실패 - 파일이 잠겨 있음
        // 임시 파일로 복사 후 처리
        TempFileName := GetTempDir + 'temp_log_' + FormatDateTime('hhnnsszzz', Now) + '.log';
        TempStream := TMemoryStream.Create;
        try
          // 임시 복사본 생성 (메모리 스트림 사용)
          try
            // Windows API를 사용하여 파일 열기
            FileHandle := FileOpen(FileName, fmOpenRead or fmShareDenyNone);

            if FileHandle <> THandle(-1) then
            begin
              try
                FileStream := TFileStream.Create(FileName, fmOpenRead or fmShareDenyNone);
                try
                  TempStream.CopyFrom(FileStream, 0);
                  TempStream.Position := 0;

                  // 임시 파일로 저장
                  TempStream.SaveToFile(TempFileName);
                finally
                  FileStream.Free;
                end;
              finally
                FileClose(FileHandle);
              end;

              // 임시 파일로부터 읽기
              AssignFile(LogFile, TempFileName);
              Reset(LogFile);

              // 임시 파일의 각 라인 처리
              while not Eof(LogFile) do
              begin
                ReadLn(LogFile, Line);

                // 로그 라인 파싱
                TimeStr := '';
                LevelStr := '';
                SourceStr := '';
                MsgStr := Line;  // 기본값은 전체 라인
                IsValidTimeFormat := False;
                IsValidLevelFormat := False;

                // [시간] [레벨] [소스] 메시지 형식 파싱
                TimeStart := Pos('[', Line);
                if TimeStart > 0 then
                begin
                  TimeEnd := Pos(']', Line, TimeStart);
                  if TimeEnd > TimeStart then
                  begin
                    TimeStr := Copy(Line, TimeStart + 1, TimeEnd - TimeStart - 1);

                    // 시간 형식 검증 - 날짜/시간 형식인지 확인
                    IsValidTimeFormat := IsValidDateTimeFormat(TimeStr);

                    if IsValidTimeFormat then  // 유효한 시간 형식일 경우에만 계속 파싱
                    begin
                      // 레벨 정보 파싱
                      LevelStart := Pos('[', Line, TimeEnd);
                      if LevelStart > 0 then
                      begin
                        LevelEnd := Pos(']', Line, LevelStart);
                        if LevelEnd > LevelStart then
                        begin
                          LevelStr := Copy(Line, LevelStart + 1, LevelEnd - LevelStart - 1);

                          // 레벨 형식 검증 - 알려진 로그 레벨인지 확인
                          IsValidLevelFormat := IsValidLogLevel(LevelStr);

                          if IsValidLevelFormat then  // 유효한 레벨 형식일 경우에만 계속 파싱
                          begin
                            // 소스 정보 파싱 (있을 경우)
                            SourceStart := Pos('[', Line, LevelEnd);
                            if SourceStart > 0 then
                            begin
                              SourceEnd := Pos(']', Line, SourceStart);
                              if SourceEnd > SourceStart then
                              begin
                                SourceStr := Copy(Line, SourceStart + 1, SourceEnd - SourceStart - 1);
                                MsgStart := SourceEnd + 1;
                              end
                              else
                                MsgStart := LevelEnd + 1;
                            end
                            else
                              MsgStart := LevelEnd + 1;

                            // 메시지 부분 파싱
                            if MsgStart <= Length(Line) then
                              MsgStr := Trim(Copy(Line, MsgStart, Length(Line)));
                          end
                          else
                          begin
                            // 레벨 형식이 아니면 원본 라인 사용
                            LevelStr := '';
                            MsgStr := Line;
                          end;
                        end;
                      end;
                    end
                    else
                    begin
                      // 시간 형식이 아니면 원본 라인 사용
                      TimeStr := '';
                      MsgStr := Line;
                    end;
                  end;
                end;

                // 노드 추가
                Node := FLogTree.AddChild(nil);
                Data := FLogTree.GetNodeData(Node);
                if Assigned(Data) then
                begin
                  // 파싱에 성공한 경우
                  if IsValidTimeFormat and IsValidLevelFormat then
                  begin
                    Data^.Message := MsgStr;  // 메시지 부분 저장
                    Data^.Source := SourceStr; // 소스 정보 저장

                    // 소스가 있으면 고유 소스 목록에 추가
                    if SourceStr <> '' then
                      AddSourceToList(SourceStr);

                    // 레벨 설정
                    Data^.Level := StringToLogLevel(LevelStr);

                    // 시간 설정
                    try
                      Data^.Time := StrToDateTime(TimeStr);
                    except
                      Data^.Time := Now;
                    end;
                  end
                  else
                  begin
                    // 파싱 실패한 경우 - 기본 구분자 기반 파싱 시도
                    if not ParseLogLineBasic(Line, TimeStr, LevelStr, SourceStr, MsgStr) then
                    begin
                      // 파싱 실패 - 전체 라인을 메시지로 처리
                      Data^.Message := Line;
                      Data^.Source := '';
                      Data^.Level := llInfo; // 기본값
                      Data^.Time := Now;
                    end
                    else
                    begin
                      // 기본 파싱 성공
                      Data^.Message := MsgStr;
                      Data^.Source := SourceStr;
                      if SourceStr <> '' then
                        AddSourceToList(SourceStr);
                      Data^.Level := StringToLogLevel(LevelStr);
                      try
                        if TimeStr <> '' then
                          Data^.Time := StrToDateTime(TimeStr)
                        else
                          Data^.Time := Now;
                      except
                        Data^.Time := Now;
                      end;
                    end;
                  end;
                end;
              end;

              // 임시 파일 닫기
              CloseFile(LogFile);
              SysUtils.DeleteFile(TempFileName);
            end
            else
            begin
              // 파일 열기 실패
              StatusBar.Panels[0].Text := '현재 로그 파일에 접근할 수 없습니다: ' + SysErrorMessage(GetLastError);
            end;
          except
            on E2: Exception do
            begin
              StatusBar.Panels[0].Text := '현재 로그 파일에 접근할 수 없습니다: ' + E2.Message;
            end;
          end;
        finally
          TempStream.Free;
          if FileExists(TempFileName) then
            SysUtils.DeleteFile(TempFileName);
        end;
      end;
    end;

    // 파일 크기 및 수정 시간 저장
    FileHandle := FileOpen(FileName, fmOpenRead or fmShareDenyNone);
    if FileHandle <> THandle(-1) then
    begin
      try
        FLastFileSize := FileSeek(FileHandle, 0, soFromEnd);
        // 파일 수정 시간 저장
        FLastFileModified := FileDateToDateTime(FileAge(FileName));
      finally
        FileClose(FileHandle);
      end;
    end;

    // 상태바 업데이트
    StatusBar.Panels[0].Text := Format('로그 파일 로드됨: %s (%d 항목)',
                                   [ExtractFileName(FileName), FLogTree.TotalCount]);

    // 로드된 파일명 저장
    FLastLoadedFileName := FileName;
  except
    on E: Exception do
      ShowMessage('로그 파일 로드 중 오류 발생: ' + E.Message);
  end;

  // 파일 정보 업데이트
  UpdateFileInfo(FileName);

  //// 모든 코드 실행 후 항상 파일 크기와 시간 저장
  //if FileExists(FileName) then
  //begin
  //  try
  //    FileHandle := FileOpen(FileName, fmOpenRead or fmShareDenyNone);
  //    if FileHandle <> THandle(-1) then
  //    begin
  //      try
  //        FLastFileSize := FileSeek(FileHandle, 0, soFromEnd);
  //        FLastFileModified := FileDateToDateTime(FileAge(FileName));
  //      finally
  //        FileClose(FileHandle);
  //      end;
  //    end;
  //  except
  //    // 파일 정보 가져오기 실패 시 무시
  //  end;
  //end;
end;

// 자동 새로고침 메뉴 추가
procedure TPrintfLogForm.AddAutoRefreshMenuItem;
var
  PopupMenu1: TPopupMenu;
  MenuItem, SubMenuItem: TMenuItem;
  i: Integer;
  RefreshRates: array[0..5] of Integer = (500, 1000, 2000, 5000, 10000, 30000);
  RefreshLabels: array[0..5] of string = ('0.5초', '1초', '2초', '5초', '10초', '30초');
begin
  // 기존 팝업 메뉴 가져오기
  PopupMenu1 := FLogTree.PopupMenu;
  if not Assigned(PopupMenu1) then
    Exit;

  // 구분선 추가
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '-';
  PopupMenu1.Items.Add(MenuItem);

  // 자동 새로고침 메뉴 추가
  MenuItem := TMenuItem.Create(PopupMenu1);
  MenuItem.Caption := '자동 새로고침';
  MenuItem.Tag := 10; // 새로고침 태그

  // 새로고침 간격 서브메뉴
  for i := 0 to 5 do
  begin
    SubMenuItem := TMenuItem.Create(MenuItem);
    SubMenuItem.Caption := RefreshLabels[i];
    SubMenuItem.Tag := RefreshRates[i];
    SubMenuItem.RadioItem := True;
    SubMenuItem.OnClick := @OnAutoRefreshMenuClick;

    if RefreshRates[i] = FAutoRefreshInterval then
      SubMenuItem.Checked := True;

    MenuItem.Add(SubMenuItem);
  end;

  // 사용 안함 옵션
  SubMenuItem := TMenuItem.Create(MenuItem);
  SubMenuItem.Caption := '-';
  MenuItem.Add(SubMenuItem);

  SubMenuItem := TMenuItem.Create(MenuItem);
  SubMenuItem.Caption := '사용 안함';
  SubMenuItem.Tag := 0;
  SubMenuItem.RadioItem := True;
  SubMenuItem.Checked := not FAutoRefreshEnabled;
  SubMenuItem.OnClick := @OnAutoRefreshMenuClick;
  MenuItem.Add(SubMenuItem);

  // 스크롤 위치 유지 옵션
  SubMenuItem := TMenuItem.Create(MenuItem);
  SubMenuItem.Caption := '-';
  MenuItem.Add(SubMenuItem);

  SubMenuItem := TMenuItem.Create(MenuItem);
  SubMenuItem.Caption := '스크롤 위치 유지';
  SubMenuItem.Tag := -2;
  SubMenuItem.Checked := FPreserveScrollPos;
  SubMenuItem.OnClick := @OnPreserveScrollPosClick;
  MenuItem.Add(SubMenuItem);

  // 지금 새로고침 옵션
  SubMenuItem := TMenuItem.Create(MenuItem);
  SubMenuItem.Caption := '-';
  MenuItem.Add(SubMenuItem);

  SubMenuItem := TMenuItem.Create(MenuItem);
  SubMenuItem.Caption := '지금 새로고침 (F5)';
  SubMenuItem.Tag := -1;
  SubMenuItem.OnClick := @OnManualRefreshClick;
  MenuItem.Add(SubMenuItem);

  // 메인 메뉴에 추가
  PopupMenu1.Items.Add(MenuItem);
end;

// 스크롤 위치 유지 옵션 클릭 이벤트
procedure TPrintfLogForm.OnPreserveScrollPosClick(Sender: TObject);
var
  MenuItem: TMenuItem;
begin
  if not (Sender is TMenuItem) then
    Exit;

  MenuItem := TMenuItem(Sender);
  FPreserveScrollPos := not FPreserveScrollPos;
  MenuItem.Checked := FPreserveScrollPos;

  if FPreserveScrollPos then
    StatusBar.Panels[0].Text := '새로고침 시 현재 스크롤 위치 유지'
  else
    StatusBar.Panels[0].Text := '새로고침 시 마지막으로 스크롤';
end;

// 자동 새로고침 메뉴 클릭 이벤트
procedure TPrintfLogForm.OnAutoRefreshMenuClick(Sender: TObject);
var
  MenuItem: TMenuItem;
  Interval: Integer;
begin
  if not (Sender is TMenuItem) then
    Exit;

  MenuItem := TMenuItem(Sender);
  Interval := MenuItem.Tag;

  if Interval > 0 then
  begin
    // 간격 설정 및 타이머 활성화
    FAutoRefreshInterval := Interval;
    FAutoRefreshTimer.Interval := Interval;
    FAutoRefreshEnabled := True;

    // 첫 새로고침 설정 초기화
    FFirstRefreshDone := False;
    FSkipNextRefresh := True; // 다음 새로고침은 건너뛰기

    // 타이머 시작
    FAutoRefreshTimer.Enabled := True;

    StatusBar.Panels[0].Text := Format('자동 새로고침 설정: %d ms', [Interval]);
  end
  else
  begin
    // 타이머 비활성화
    FAutoRefreshEnabled := False;
    FAutoRefreshTimer.Enabled := False;

    StatusBar.Panels[0].Text := '자동 새로고침 비활성화됨';
  end;

  // 메뉴 항목 체크 상태 업데이트
  UpdateAutoRefreshMenuChecks(Interval);
end;

// 메뉴 항목 체크 상태 업데이트
procedure TPrintfLogForm.UpdateAutoRefreshMenuChecks(SelectedInterval: Integer);
var
  PopupMenu1: TPopupMenu;
  MenuItem, SubMenuItem: TMenuItem;
  i, j: Integer;
begin
  PopupMenu1 := FLogTree.PopupMenu;
  if not Assigned(PopupMenu1) then
    Exit;

  // 자동 새로고침 메뉴 찾기
  for i := 0 to PopupMenu1.Items.Count - 1 do
  begin
    MenuItem := PopupMenu1.Items[i];
    if MenuItem.Tag = 10 then // 새로고침 태그로 식별
    begin
      // 서브메뉴 체크 상태 업데이트
      for j := 0 to MenuItem.Count - 1 do
      begin
        SubMenuItem := MenuItem.Items[j];
        if SubMenuItem.RadioItem then // 라디오 항목만 체크 상태 변경
        begin
          SubMenuItem.Checked := (SubMenuItem.Tag = SelectedInterval);
        end;
      end;
      Break;
    end;
  end;
end;

// 타이머 이벤트 핸들러
procedure TPrintfLogForm.OnAutoRefreshTimer(Sender: TObject);
begin
  if FIsFileViewMode and FAutoRefreshEnabled and (FLastLoadedFileName <> '') then
  begin
    // 첫 번째 새로고침 건너뛰기
    if FSkipNextRefresh then
    begin
      FSkipNextRefresh := False;
      StatusBar.Panels[0].Text := Format('첫 새로고침 초기화 중... (%s)',
                                       [FormatDateTime('hh:nn:ss', Now)]);

      // 현재 파일 정보 갱신 (비교 기준점 설정)
      UpdateFileInfo(FLastLoadedFileName);
      Exit;
    end;

    ReloadLogFile;
  end;
end;

// 파일 정보 갱신
procedure TPrintfLogForm.UpdateFileInfo(const FileName: string);
var
  FileHandle: THandle;
begin
  if not FileExists(FileName) then
    Exit;

  try
    FileHandle := FileOpen(FileName, fmOpenRead or fmShareDenyNone);
    if FileHandle <> THandle(-1) then
    begin
      try
        FLastFileSize := FileSeek(FileHandle, 0, soFromEnd);
        FLastFileModified := FileDateToDateTime(FileAge(FileName));
      finally
        FileClose(FileHandle);
      end;
    end;
  except
    // 파일 정보 가져오기 실패 시 무시
  end;
end;

// 파일 속성 변경 확인
function TPrintfLogForm.HasFileAttributesChanged(const FileName: string): Boolean;
var
  FileHandle: THandle;
  CurrentSize: Int64;
  CurrentModified: TDateTime;
begin
  Result := False;

  if not FileExists(FileName) then
    Exit;

  try
    FileHandle := FileOpen(FileName, fmOpenRead or fmShareDenyNone);
    if FileHandle <> THandle(-1) then
    begin
      try
        CurrentSize := FileSeek(FileHandle, 0, soFromEnd);
        CurrentModified := FileDateToDateTime(FileAge(FileName));

        // 파일 크기나 수정 시간이 변경되었는지 확인
        Result := (CurrentSize <> FLastFileSize) or
                 (Abs(CurrentModified - FLastFileModified) > 0.000001);

        // 변경되었다면 정보 업데이트
        if Result then
        begin
          FLastFileSize := CurrentSize;
          FLastFileModified := CurrentModified;
        end;
      finally
        FileClose(FileHandle);
      end;
    end;
  except
    // 오류 발생 시 변경된 것으로 간주
    Result := True;
  end;
end;

// 수동 새로고침 메뉴 클릭 이벤트
procedure TPrintfLogForm.OnManualRefreshClick(Sender: TObject);
begin
  if FIsFileViewMode and (FLastLoadedFileName <> '') then
  begin
    ReloadLogFile;
    StatusBar.Panels[0].Text := '로그 파일 새로고침 완료: ' +
                               FormatDateTime('hh:nn:ss', Now);
  end;
end;

// 파일 변경 감지 함수 - 파일 끝 부분만 읽어서 비교
// 파일 변경 감지 함수 - 파일 끝 부분만 읽어서 비교
function TPrintfLogForm.HasFileContentChanged(const FileName: string): Boolean;
const
  MAX_LINES_TO_CHECK = 20;       // 확인할 마지막 줄 수
  MAX_BUFFER_SIZE = 32 * 1024;   // 읽을 최대 버퍼 크기 (32KB)
  FULL_READ_THRESHOLD = 3 * 1024 * 1024; // 3MB 이상은 끝부분만 읽기
var
  FileHandle: THandle;
  FileSize: Int64;
  CurrentModified: TDateTime;
  LineBuffer: array[0..MAX_LINES_TO_CHECK-1] of string;
  LineCount, i: Integer;
  FileStream: TFileStream;
  TempFileName: string;
  LogFile: TextFile;
  Buffer: array[0..MAX_BUFFER_SIZE-1] of Byte;
  BytesRead: Integer;
  CurrentLine: string;
  LastLinesEqual: Boolean;
  FileAge: Integer;
begin
  Result := True; // 기본값: 변경되었다고 가정

  if not FileExists(FileName) then
    Exit;

  // 파일 크기와 수정 시간 확인 (빠른 체크)
  try
    FileHandle := FileOpen(FileName, fmOpenRead or fmShareDenyNone);
    if FileHandle <> THandle(-1) then
    begin
      try
        FileSize := FileSeek(FileHandle, 0, soFromEnd);
        FileAge := SysUtils.FileAge(FileName);
        if FileAge <> -1 then
          CurrentModified := FileDateToDateTime(FileAge)
        else
          CurrentModified := Now;

        // 파일 크기와 수정 시간이 같으면 변경 없음으로 처리
        if (FileSize = FLastFileSize) and
           (Abs(CurrentModified - FLastFileModified) < 0.000001) then
        begin
          Result := False;
          Exit;
        end;

        // 정보 업데이트
        FLastFileSize := FileSize;
        FLastFileModified := CurrentModified;
      finally
        FileClose(FileHandle);
      end;
    end;
  except
    on E: Exception do
    begin
      // 파일 정보 가져오기 실패 - 변경된 것으로 간주
      Exit(True);
    end;
  end;

  // 대용량 파일이면 내용 끝부분만 임시 파일에 복사해서 읽기
  try
    // 1. 먼저 파일의 일부를 읽어서 임시 파일로 저장
    if FileSize > FULL_READ_THRESHOLD then
    begin
      FileStream := TFileStream.Create(FileName, fmOpenRead or fmShareDenyNone);
      try
        // 파일 끝부분을 읽기 위해 위치 조정
        if FileStream.Size > MAX_BUFFER_SIZE then
          FileStream.Position := FileStream.Size - MAX_BUFFER_SIZE;

        // 버퍼에 읽기
        BytesRead := FileStream.Read(Buffer, SizeOf(Buffer));

        // 임시 파일로 저장
        TempFileName := GetTempDir + 'temp_log_tail_' + FormatDateTime('hhnnsszzz', Now) + '.log';

        with TFileStream.Create(TempFileName, fmCreate) do
        try
          Write(Buffer, BytesRead);
        finally
          Free;
        end;
      finally
        FileStream.Free;
      end;
    end
    else
      TempFileName := FileName; // 작은 파일은 원본 사용

    // 2. 임시 파일이나 원본 파일을 라인 단위로 읽기
    try
      AssignFile(LogFile, TempFileName);
      Reset(LogFile);

      // 링 버퍼 초기화
      for i := 0 to MAX_LINES_TO_CHECK-1 do
        LineBuffer[i] := '';

      // 파일 라인들을 읽어서 마지막 N줄 저장
      LineCount := 0;
      while not Eof(LogFile) do
      begin
        ReadLn(LogFile, CurrentLine);
        LineBuffer[LineCount mod MAX_LINES_TO_CHECK] := CurrentLine;
        Inc(LineCount);
      end;

      CloseFile(LogFile);

      // 3. 이전에 저장된 마지막 줄들과 비교
      if Length(FLastLineBuffer) > 0 then
      begin
        LastLinesEqual := True;

        // 줄 수가 다르면 내용이 변경된 것
        if LineCount < Length(FLastLineBuffer) then
          LastLinesEqual := False
        else
        begin
          // 마지막 N줄 비교
          for i := 0 to Min(MAX_LINES_TO_CHECK, Length(FLastLineBuffer))-1 do
          begin
            if i >= LineCount then
              Break;

            // 시간 컬럼 등을 포함한 전체 라인 비교
            if LineBuffer[(LineCount - i - 1) mod MAX_LINES_TO_CHECK] <>
               FLastLineBuffer[i] then
            begin
              LastLinesEqual := False;
              Break;
            end;
          end;
        end;

        // 마지막 N줄이 같으면 변경 없음으로 판단
        if LastLinesEqual then
          Result := False;
      end;

      // 4. 현재 마지막 줄 저장 (다음 비교를 위해)
      SetLength(FLastLineBuffer, Min(LineCount, MAX_LINES_TO_CHECK));
      for i := 0 to Min(LineCount, MAX_LINES_TO_CHECK)-1 do
        FLastLineBuffer[i] := LineBuffer[(LineCount - i - 1) mod MAX_LINES_TO_CHECK];

    finally
      // 임시 파일 정리
      if (TempFileName <> FileName) and FileExists(TempFileName) then
        SysUtils.DeleteFile(TempFileName);
    end;
  except
    on E: Exception do
    begin
      // 파일 읽기 오류 발생 시 기본값 사용 (변경된 것으로 간주)
      Result := True;

      // 임시 파일 정리
      if (TempFileName <> FileName) and FileExists(TempFileName) then
        SysUtils.DeleteFile(TempFileName);
    end;
  end;
end;

// 로그 파일 다시 로드 메서드 수정
procedure TPrintfLogForm.ReloadLogFile;
var
  FirstVisibleNode: PVirtualNode;
  VisibleIndex: Integer;
  HasChanges: Boolean;
  FileSize: Int64;
begin
  if not FIsFileViewMode or (FLastLoadedFileName = '') then
    Exit;

  // 첫 번째 새로고침 건너뛰기
  if FSkipNextRefresh then
  begin
    FSkipNextRefresh := False;
    StatusBar.Panels[0].Text := Format('새로고침 초기화 중... (%s)',
                               [FormatDateTime('hh:nn:ss', Now)]);

    // 파일 정보만 갱신
    if FileExists(FLastLoadedFileName) then
    begin
      // 파일 크기가 크면 자동 새로고침 간격 조정
      FileSize := GetFileSize(FLastLoadedFileName);
      if FileSize > 3 * 1024 * 1024 then // 3MB 이상
      begin
        if FAutoRefreshInterval < 5000 then // 5초 미만이면
        begin
          FAutoRefreshInterval := 10000; // 10초로 설정
          FAutoRefreshTimer.Interval := FAutoRefreshInterval;
          StatusBar.Panels[0].Text := Format('대용량 파일 감지: 새로고침 간격을 %d초로 조정',
                                         [FAutoRefreshInterval div 1000]);
        end;
      end;
    end;

    Exit;
  end;

  // 파일 변경 확인
  HasChanges := HasFileContentChanged(FLastLoadedFileName);

  if not HasChanges then
  begin
    StatusBar.Panels[0].Text := Format('파일에 변경 없음 (마지막 확인: %s)',
                               [FormatDateTime('hh:nn:ss', Now)]);
    Exit;
  end;

  // 현재 스크롤 위치 저장
  if FPreserveScrollPos then
  begin
    FirstVisibleNode := FLogTree.GetFirstVisible;
    if Assigned(FirstVisibleNode) then
      FLastVisibleNode := FirstVisibleNode;
  end;

  // 변경사항이 있을 때만 실제 리로드 수행
  if HasChanges then
  begin
    // 원본 로드 함수 호출
    LoadLogsFromFile(FLastLoadedFileName);

    // 스크롤 위치 복원
    if FPreserveScrollPos and Assigned(FLastVisibleNode) then
    begin
      // 대략적인 위치 계산 (예: 1/3 지점)
      if FLogTree.TotalCount > 0 then
      begin
        VisibleIndex := FLogTree.RootNodeCount div 3;
        FirstVisibleNode := FLogTree.GetFirst;

        while Assigned(FirstVisibleNode) and (VisibleIndex > 0) do
        begin
          FirstVisibleNode := FLogTree.GetNext(FirstVisibleNode);
          Dec(VisibleIndex);
        end;

        if Assigned(FirstVisibleNode) then
          FLogTree.ScrollIntoView(FirstVisibleNode, False);
      end;
    end
    else
    begin
      // 마지막 노드로 스크롤
      FirstVisibleNode := FLogTree.GetLast;
      if Assigned(FirstVisibleNode) then
        FLogTree.ScrollIntoView(FirstVisibleNode, True);
    end;

    StatusBar.Panels[0].Text := Format('로그 파일 새로고침 완료: %s',
                               [FormatDateTime('hh:nn:ss', Now)]);
  end;

  // 첫 번째 새로고침 완료 플래그 설정
  FFirstRefreshDone := True;
end;

// 파일 크기 가져오기 함수
function TPrintfLogForm.GetFileSize(const FileName: string): Int64;
var
  FileHandle: THandle;
begin
  Result := 0;

  if not FileExists(FileName) then
    Exit;

  FileHandle := FileOpen(FileName, fmOpenRead or fmShareDenyNone);
  if FileHandle <> THandle(-1) then
  begin
    try
      Result := FileSeek(FileHandle, 0, soFromEnd);
    finally
      FileClose(FileHandle);
    end;
  end;
end;

// 날짜/시간 형식 검증 함수
function TPrintfLogForm.IsValidDateTimeFormat(const DateTimeStr: string): Boolean;
var
  DateTime: TDateTime;
begin
  Result := False;

  try
    // 날짜/시간 형식인지 확인
    DateTime := StrToDateTime(DateTimeStr);
    Result := True;
  except
    // 날짜/시간 형식이 아니면 False 반환
    Result := False;
  end;
end;

// 로그 레벨 검증 함수
function TPrintfLogForm.IsValidLogLevel(const LevelStr: string): Boolean;
var
  UpperLevelStr: string;
begin
  UpperLevelStr := UpperCase(LevelStr);

  // 알려진 로그 레벨인지 확인
  Result := (UpperLevelStr = 'INFO') or
            (UpperLevelStr = 'DEBUG') or
            (UpperLevelStr = 'DEVEL') or
            (UpperLevelStr = 'WARNING') or
            (UpperLevelStr = 'WARN') or
            (UpperLevelStr = 'ERROR') or
            (UpperLevelStr = 'FATAL');
end;

// 문자열 로그 레벨을 TLogLevel로 변환
function TPrintfLogForm.StringToLogLevel(const LevelStr: string): TLogLevel;
var
  UpperLevelStr: string;
begin
  UpperLevelStr := UpperCase(LevelStr);

  if UpperLevelStr = 'INFO' then
    Result := llInfo
  else if UpperLevelStr = 'DEBUG' then
    Result := llDebug
  else if (UpperLevelStr = 'WARN') or (UpperLevelStr = 'WARNING') then
    Result := llWarning
  else if UpperLevelStr = 'ERROR' then
    Result := llError
  else if UpperLevelStr = 'FATAL' then
    Result := llFatal
  else if UpperLevelStr = 'DEVEL' then
    Result := llDevelop
  else
    Result := llInfo; // 기본값
end;

// 기본 구분자 기반의 로그 라인 파싱 (백업 메서드)
function TPrintfLogForm.ParseLogLineBasic(const LogLine: string;
                                        out TimeStr, LevelStr, SourceStr, MsgStr: string): Boolean;
const
  TimeSeparators: array[0..2] of Char = (':', '.', '-');
var
  Parts: TStringList;
  i, TimePart: Integer;
  HasTime, HasLevel: Boolean;
  WordStart, WordEnd, WordLen: Integer;
  Word: string;
begin
  Result := False;
  TimeStr := '';
  LevelStr := '';
  SourceStr := '';
  MsgStr := LogLine;

  // 명확한 구분자가 없는 경우 단어 기반으로 파싱 시도
  Parts := TStringList.Create;
  try
    // 공백으로 분리 (WordCount/ExtractWord 대신 직접 구현)
    WordStart := 1;
    while WordStart <= Length(LogLine) do
    begin
      // 앞쪽 공백 건너뛰기
      while (WordStart <= Length(LogLine)) and
            ((LogLine[WordStart] = ' ') or (LogLine[WordStart] = #9)) do
        Inc(WordStart);

      if WordStart > Length(LogLine) then
        Break;

      // 단어 끝 찾기
      WordEnd := WordStart;
      while (WordEnd <= Length(LogLine)) and
            (LogLine[WordEnd] <> ' ') and (LogLine[WordEnd] <> #9) do
        Inc(WordEnd);

      // 단어 추출 및 추가
      WordLen := WordEnd - WordStart;
      if WordLen > 0 then
      begin
        Word := Copy(LogLine, WordStart, WordLen);
        Parts.Add(Word);
      end;

      WordStart := WordEnd + 1;
    end;

    if Parts.Count < 2 then
      Exit(False); // 최소 2개 부분(시간 + 메시지)이 있어야 함

    // 시간 부분 찾기 (첫 번째 또는 두 번째 토큰)
    HasTime := False;
    for TimePart := 0 to Min(1, Parts.Count - 1) do
    begin
      // 시간 형식인지 확인 (콜론이나 점 포함)
      for i := Low(TimeSeparators) to High(TimeSeparators) do
      begin
        if Pos(TimeSeparators[i], Parts[TimePart]) > 0 then
        begin
          TimeStr := Parts[TimePart];
          HasTime := True;
          Break;
        end;
      end;

      if HasTime then
        Break;
    end;

    // 레벨 부분 찾기 (다음 토큰)
    HasLevel := False;
    if HasTime and (TimePart + 1 < Parts.Count) then
    begin
      if IsValidLogLevel(Parts[TimePart + 1]) then
      begin
        LevelStr := Parts[TimePart + 1];
        HasLevel := True;
      end;
    end;

    // 소스 부분 찾기 (레벨 다음 토큰, 없으면 건너뜀)
    if HasLevel and (TimePart + 2 < Parts.Count) then
    begin
      // 소스는 특별한 형식이 없으므로 그냥 다음 토큰으로 간주
      SourceStr := Parts[TimePart + 2];

      // 메시지는 그 이후 모든 부분
      MsgStr := '';
      for i := TimePart + 3 to Parts.Count - 1 do
      begin
        if i = TimePart + 3 then
          MsgStr := Parts[i]  // 첫 부분은 공백 없이 시작
        else
          MsgStr := MsgStr + ' ' + Parts[i];
      end;
    end
    else if HasLevel then
    begin
      // 소스 없음, 메시지는 레벨 이후 모든 부분
      MsgStr := '';
      for i := TimePart + 2 to Parts.Count - 1 do
      begin
        if i = TimePart + 2 then
          MsgStr := Parts[i]  // 첫 부분은 공백 없이 시작
        else
          MsgStr := MsgStr + ' ' + Parts[i];
      end;
    end
    else if HasTime then
    begin
      // 레벨도 없음, 메시지는 시간 이후 모든 부분
      MsgStr := '';
      for i := TimePart + 1 to Parts.Count - 1 do
      begin
        if i = TimePart + 1 then
          MsgStr := Parts[i]  // 첫 부분은 공백 없이 시작
        else
          MsgStr := MsgStr + ' ' + Parts[i];
      end;
    end;

    // 시간 정보가 있으면 기본 파싱 성공
    Result := HasTime;
  finally
    Parts.Free;
  end;
end;


// 파일 뷰 모드용 생성자
constructor TPrintfLogForm.CreateForFileViewing(AOwner: TComponent; const FileName: string);
begin
  // 기본 생성자 호출 (nil 핸들러로 로깅 모드 아님을 표시)
  Create(AOwner, nil);

  // 읽기 전용 모드 설정
  Caption := '로그 파일 보기: ' + ExtractFileName(FileName);
  FIsFileViewMode := True;

  // 파일 뷰어 모드에서 시간 칼럼 너비 조정 (날짜 포함 형식)
  FLogTree.BeginUpdate;
  try
    // 시간 칼럼 너비 더 넓게 확장 (yyyy-mm-dd hh:nn:ss.zzz 형식 표시용)
    FLogTree.Header.Columns[0].Width := 180;  // 더 넓게 (날짜 포함)
    FLogTree.Header.Columns[0].MinWidth := 174;
    FLogTree.InvalidateColumn(0);

    // 소스 칼럼은 그대로 유지 (65px)

    // 레이아웃 갱신
    FLogTree.Invalidate;
  finally
    FLogTree.EndUpdate;
  end;

  // 트레이 아이콘 불필요
  if Assigned(FTrayIcon) then
  begin
    FTrayIcon.Free;
    FTrayIcon := nil;
  end;

  // 검색 및 필터링 패널 생성
  CreateSearchPanel;
  CreateFilterPanel;
  CreateColumnVisibilityPanel;

  // 상태바 조정 - 단축키 정보 표시
  if Assigned(FStatusBar) then
  begin
    FStatusBar.Panels[0].Width := 350;
    FStatusBar.Panels[1].Text := '검색: Ctrl+F, 필터: Ctrl+T, 컬럼: Ctrl+V, 통계: Ctrl+G';
  end;

  // 파일에서 로그 로드
  LoadLogsFromFile(FileName);

  // 초기 필터 옵션 설정
  FFilterOptions.Enabled := False;
  FFilterOptions.ShowInfo := True;
  FFilterOptions.ShowDebug := True;
  FFilterOptions.ShowWarning := True;
  FFilterOptions.ShowError := True;
  FFilterOptions.ShowFatal := True;
  FFilterOptions.ShowDevelop := True;
  FFilterOptions.FilterTimeEnabled := False;
  FFilterOptions.FilterTextEnabled := False;
  FFilterOptions.FilterSourceEnabled := False; // 소스 필터 초기 비활성화

  // 초기 검색 옵션 설정
  FSearchOptions.MatchCase := False;
  FSearchOptions.WholeWord := False;
  FSearchOptions.SearchInTime := True;
  FSearchOptions.SearchInLevel := True;
  FSearchOptions.SearchInMessage := True;
  FSearchOptions.SearchInSource := True; // 소스 검색 활성화

  // 컬럼 가시성 초기 설정
  FColumnVisibility.ShowTimeColumn := True;
  FColumnVisibility.ShowLevelColumn := True;
  FColumnVisibility.ShowSourceColumn := True;
  FColumnVisibility.ShowMessageColumn := True;

  // 원본 노드 저장
  StoreOriginalNodes;

  // 자동 새로고침 설정 초기화
  FAutoRefreshEnabled := False;
  FAutoRefreshInterval := 1000; // 기본 1초
  FLastLoadedFileName := FileName;
  FPreserveScrollPos := True;
  // 파일 관련 변수 초기화
  FLastFileSize := 0;
  FLastFileModified := 0;
  SetLength(FLastLineBuffer, 0); // 빈 배열로 초기화
  FFirstRefreshDone := False;
  FSkipNextRefresh := True; // 첫 번째 새로고침 건너뛰기 활성화

  // 타이머 생성
  FAutoRefreshTimer := TTimer.Create(Self);
  FAutoRefreshTimer.Enabled := False;
  FAutoRefreshTimer.Interval := FAutoRefreshInterval;
  FAutoRefreshTimer.OnTimer := @OnAutoRefreshTimer;

  // 메뉴에 자동 새로고침 옵션 추가
  AddAutoRefreshMenuItem;
end;

{ TPrintfHandler }

constructor TPrintfHandler.Create;
var
  BasePath, DatePath, PrintfPath: string;
  CurrentDate: TDateTime;
begin
  inherited Create;

  // 기본 설정
  FVisible := True;
  FFormPosition := fpTopRight;
  FFormWidth := 700;  // 폼 너비 증가
  FFormHeight := 500; // 폼 높이 증가
  FFormCaption := '로그 출력';
  FStayOnTop := False;
  FAutoSave := False;
  FShowTime := True;
  FMaxLogCount := 5000;
  FColorByLevel := True;
  FColorBySource := False; // 기본적으로 소스별 색상 비활성화
  FShowDateInTimeColumn := False; // 기본값으로 시간만 표시

  // 컬럼 가시성 기본 설정
  FColumnVisibility.ShowTimeColumn := True;
  FColumnVisibility.ShowLevelColumn := True;
  FColumnVisibility.ShowSourceColumn := True; // 기본적으로 소스 컬럼 표시
  FColumnVisibility.ShowMessageColumn := True;

  // 소스 색상 맵 초기화
  FSourceColorMap := TStringList.Create;
  FSourceColorMap.Sorted := True;
  FSourceColorMap.Duplicates := dupIgnore;

  // 로그 로테이션 기본 설정
  FAutoRotate := False;
  FRotateLineCount := 5000;
  FRotateSaveBeforeClear := True;

  // 날짜별 디렉토리 구조 사용하여 로그 폴더 설정
  BasePath := ExtractFilePath(ParamStr(0)) + 'logs';
  CurrentDate := Date;
  DatePath := IncludeTrailingPathDelimiter(BasePath) +
             FormatDateTime('yyyy', CurrentDate) + PathDelim +
             FormatDateTime('mm', CurrentDate) + PathDelim +
             FormatDateTime('dd', CurrentDate);
  PrintfPath := DatePath + PathDelim + 'Printf';

  // 기본 로그 저장 폴더 설정
  FRotateLogFolder := PrintfPath;

  // 디렉토리가 없으면 생성 (Init에서도 확인)
  if not DirectoryExists(PrintfPath) then
    ForceDirectories(PrintfPath);

  // 로그 폼 생성 및 설정
  if Application <> nil then
  begin
    FLogForm := TPrintfLogForm.Create(Application, Self);
    FLogForm.OnClear := @HandleFormClear;
    FLogForm.OnSave := @HandleFormSave;
    FLogForm.Width := FFormWidth;
    FLogForm.Height := FFormHeight;
    FLogForm.Caption := FFormCaption;

    // 위치 설정
    SetFormPosition(FFormPosition);
  end;

  // 비동기 처리 관련
  FLogQueue := TThreadList.Create;
  FAsyncThread := nil;
  FQueueMaxSize := 1000;       // 기본 큐 크기
  FQueueFlushInterval := 1000; // 기본 플러시 간격 (ms)
  FLastQueueFlush := Now;
end;


destructor TPrintfHandler.Destroy;
begin
  Shutdown;

  // 로그 큐 정리
  FlushQueue;
  FLogQueue.Free;

  // 소스 색상 맵 해제
  if Assigned(FSourceColorMap) then
    FSourceColorMap.Free;

  // 폼 정리
  if Assigned(FLogForm) then
  begin
    FLogForm.Free;
    FLogForm := nil;
  end;

  inherited;
end;

procedure TPrintfHandler.Init;
begin
  inherited;

  // 로그 폴더 확인 및 생성
  if FRotateLogFolder <> '' then
    if not DirectoryExists(FRotateLogFolder) then
      ForceDirectories(FRotateLogFolder);

  // 폼이 생성되었는지 확인
  if not Assigned(FLogForm) and (Application <> nil) then
  begin
    FLogForm := TPrintfLogForm.Create(Application, Self);
    FLogForm.OnClear := @HandleFormClear;
    FLogForm.OnSave := @HandleFormSave;
    FLogForm.Width := FFormWidth;
    FLogForm.Height := FFormHeight;
    FLogForm.Caption := FFormCaption;

    // 위치 설정
    SetFormPosition(FFormPosition);
  end;

  // 항상 위에 표시 설정
  if Assigned(FLogForm) then
  begin
    if FStayOnTop then
      FLogForm.FormStyle := fsStayOnTop
    else
      FLogForm.FormStyle := fsNormal;

    // 폼 표시 설정
    if FVisible then
      FLogForm.Show;
  end;

  // 현재 로그 보기 메뉴 항목 추가
  AddViewCurrentLogMenuItem;
end;

procedure TPrintfHandler.Shutdown;
begin
  // 비동기 스레드 종료
  SetAsyncMode(amNone);

  // 로그 큐 플러시
  FlushQueue;

  // 자동 저장 옵션이 켜져 있으면 저장
  if FAutoSave and Assigned(FLogForm) and (FLogForm.LogTree.TotalCount > 0) then
    SaveToFile;

  // 폼 숨기기 및 정리
  if Assigned(FLogForm) then
    FLogForm.Hide;

  inherited;
end;

// 소스별 색상 관련 메서드 추가
function TPrintfHandler.GetColorForSource(const Source: string): TColor;
var
  Index: Integer;
  ColorStr: string;
begin
  Result := clNone; // 기본값은 색상 없음

  if not FColorBySource or (Source = '') then
    Exit;

  // 소스 색상 맵에서 찾기
  Index := FSourceColorMap.IndexOf(Source);
  if Index >= 0 then
  begin
    ColorStr := FSourceColorMap.ValueFromIndex[Index];
    if ColorStr <> '' then
      Result := StrToIntDef(ColorStr, clNone);
  end;
end;

procedure TPrintfHandler.SetSourceColor(const Source: string; Color: TColor);
begin
  if Source = '' then
    Exit;

  // 소스 색상 저장
  FSourceColorMap.Values[Source] := IntToStr(Color);

  // 폼이 있으면 갱신
  if Assigned(FLogForm) then
    FLogForm.LogTree.Invalidate;
end;

procedure TPrintfHandler.ClearSourceColors;
begin
  FSourceColorMap.Clear;

  // 폼이 있으면 갱신
  if Assigned(FLogForm) then
    FLogForm.LogTree.Invalidate;
end;

// 소스 통계 표시
procedure TPrintfHandler.ShowSourceStatistics;
begin
  if Assigned(FLogForm) then
    FLogForm.ShowSourceStatisticsForm;
end;

// 컬럼 가시성 관련
procedure TPrintfHandler.SetColumnVisibility(const Value: TColumnVisibility);
begin
  FColumnVisibility := Value;

  // 로그 폼에 설정 적용
  if Assigned(FLogForm) then
  begin
    FLogForm.ColumnVisibility := Value;
    FLogForm.ApplyColumnVisibility;
  end;
end;


// 현재 로깅 중인 파일 열기 기능
procedure TPrintfHandler.ViewCurrentLogFile;
var
  LogFileName: string;
  TempFileName: string;
  ViewerForm: TPrintfLogForm;
begin
  if not Assigned(FLogForm) then
    Exit;

  // 현재 로그 파일이 없는 경우 메모리에 있는 내용으로 임시 파일 생성 후 보기
  if (not FAutoSave) or (FRotateLogFolder = '') then
  begin
    TempFileName := GetTempDir + 'current_log_' + FormatDateTime('yyyymmddhhnnsszzz', Now) + '.log';

    // 현재 내용을 임시 파일에 저장
    SaveToFile(TempFileName);

    // 파일 뷰어 폼 생성
    ViewerForm := TPrintfLogForm.CreateForFileViewing(Application, TempFileName);
    ViewerForm.Caption := '현재 로그 보기 (임시 파일)';
    ViewerForm.Show;

    Exit;
  end;

  // 현재 로그 파일 경로 확인
  LogFileName := '';

  // 1. 현재 날짜 기반 파일명 구성
  LogFileName := IncludeTrailingPathDelimiter(FRotateLogFolder) +
               FormatDateTime('yyyymmdd', Now) + '.log';

  // 파일이 없으면 다른 이름 패턴 찾기
  if not FileExists(LogFileName) then
  begin
    LogFileName := IncludeTrailingPathDelimiter(FRotateLogFolder) +
                 FormatDateTime('yyyymmdd-hhnnss', Now) + '.log';

    if not FileExists(LogFileName) then
    begin
      // 디렉토리에서 가장 최근 로그 파일 찾기
      LogFileName := FindMostRecentLogFile(FRotateLogFolder);

      if LogFileName = '' then
      begin
        // 로그 파일을 찾을 수 없음 - 임시 파일 생성
        TempFileName := GetTempDir + 'current_log_' + FormatDateTime('yyyymmddhhnnsszzz', Now) + '.log';
        SaveToFile(TempFileName);
        LogFileName := TempFileName;
      end;
    end;
  end;

  // 파일 뷰어 폼 열기
  ViewerForm := TPrintfLogForm.CreateForFileViewing(Application, LogFileName);
  ViewerForm.Caption := '현재 로그 보기: ' + ExtractFileName(LogFileName);

  // 자동 새로고침 설정 (필요시 활성화)
  // 타이머 기반 자동 새로고침은 TPrintfLogForm에 구현되어 있음

  ViewerForm.Show;
end;

// 가장 최근의 로그 파일 찾기
function TPrintfHandler.FindMostRecentLogFile(const LogFolder: string): string;
var
  SearchRec: TSearchRec;
  MostRecentTime: TDateTime;
  MostRecentFile: string;
  FilePath: string;
  FileTime: TDateTime;
  FileAge: Integer;
begin
  Result := '';

  if not DirectoryExists(LogFolder) then
    Exit;

  MostRecentTime := 0;
  MostRecentFile := '';

  // 디렉토리 내 모든 .log 파일 검색
  if FindFirst(IncludeTrailingPathDelimiter(LogFolder) + '*.log', faAnyFile, SearchRec) = 0 then
  begin
    try
      repeat
        FilePath := IncludeTrailingPathDelimiter(LogFolder) + SearchRec.Name;
        FileAge := SysUtils.FileAge(FilePath);
        if FileAge <> -1 then
        begin
          FileTime := FileDateToDateTime(FileAge);

          if FileTime > MostRecentTime then
          begin
            MostRecentTime := FileTime;
            MostRecentFile := FilePath;
          end;
        end;
      until FindNext(SearchRec) <> 0;
    finally
      SysUtils.FindClose(SearchRec);
    end;
  end;

  Result := MostRecentFile;
end;

// TPrintfHandler 클래스 내 메인 메뉴에 항목 추가
procedure TPrintfHandler.AddViewCurrentLogMenuItem;
var
  LogMenu: TMenuItem;
  MenuItem: TMenuItem;
begin
  if not Assigned(FLogForm) or not Assigned(FLogForm.LogTree) or
     not Assigned(FLogForm.LogTree.PopupMenu) then
    Exit;

  // 로그 보기 메뉴 항목 추가
  MenuItem := TMenuItem.Create(FLogForm.LogTree.PopupMenu);
  MenuItem.Caption := '현재 로그 파일 보기';
  MenuItem.Tag := 7; // 현재 로그 보기 태그
  MenuItem.OnClick := @ViewCurrentLogMenuClick;

  // 적절한 위치에 메뉴 삽입
  FLogForm.LogTree.PopupMenu.Items.Insert(
    FLogForm.LogTree.PopupMenu.Items.Count - 1, MenuItem);  // 마지막 항목 앞에 삽입
end;

// 메뉴 클릭 이벤트 핸들러
procedure TPrintfHandler.ViewCurrentLogMenuClick(Sender: TObject);
begin
  ViewCurrentLogFile;
end;


procedure TPrintfHandler.ConfigureColumnVisibility(ShowTime, ShowLevel, ShowSource, ShowMessage: Boolean);
var
  NewVisibility: TColumnVisibility;
begin
  NewVisibility.ShowTimeColumn := ShowTime;
  NewVisibility.ShowLevelColumn := ShowLevel;
  NewVisibility.ShowSourceColumn := ShowSource;
  NewVisibility.ShowMessageColumn := ShowMessage;

  SetColumnVisibility(NewVisibility);
end;

procedure TPrintfHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  LogItem: PPrintfLogQueueItem;
  List: TList;
  SourceStr: string;
  CleanMsg: string;
  TimeStart, TimeEnd, LevelStart, LevelEnd, SourceStart, SourceEnd: Integer;
begin
  // 소스 식별자 가져오기
  SourceStr := SourceIdentifier;

  // 메시지 정제 - 이미 [시간] [레벨] [소스] 형식으로 되어 있는지 확인
  CleanMsg := Msg;

  // 메시지에서 이미 [시간] [레벨] [소스] 형식이 있는지 확인하고 제거
  if (Length(Msg) > 2) and (Msg[1] = '[') then
  begin
    TimeStart := 1;
    TimeEnd := Pos(']', Msg, TimeStart);

    if TimeEnd > TimeStart then
    begin
      LevelStart := Pos('[', Msg, TimeEnd);
      if LevelStart > 0 then
      begin
        LevelEnd := Pos(']', Msg, LevelStart);

        if LevelEnd > LevelStart then
        begin
          SourceStart := Pos('[', Msg, LevelEnd);
          if SourceStart > 0 then
          begin
            SourceEnd := Pos(']', Msg, SourceStart);

            if (SourceEnd > SourceStart) and (SourceEnd < Length(Msg)) then
            begin
              // [시간] [레벨] [소스] 형식의 접두어가 있음 - 실제 메시지만 추출
              CleanMsg := Trim(Copy(Msg, SourceEnd + 1, Length(Msg)));
            end;
          end;
        end;
      end;
    end;
  end;

  if AsyncMode = amThread then
  begin
    // 비동기 모드: 큐에 메시지 추가
    New(LogItem);
    LogItem^.Message := CleanMsg; // 정제된 메시지 사용
    LogItem^.Level := Level;
    LogItem^.Source := SourceStr; // 소스 정보

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
    // 동기 모드: 직접 로그 추가
    if Assigned(FLogForm) then
    begin
      try
        FLogForm.AddLog(CleanMsg, Level, SourceStr); // 정제된 메시지 및 소스 정보 전달
      except
        // 추가 실패 무시
      end;
    end;
  end;
end;

procedure TPrintfHandler.WriteSessionStart;
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

procedure TPrintfHandler.SetAsyncMode(Mode: TAsyncMode);
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
          FAsyncThread := TPrintfHandlerThread.Create(Self);
      end;
  end;
end;

function TPrintfHandler.GetColorForLevel(Level: TLogLevel): TColor;
begin
  case Level of
    llDevelop:  Result := TColor($AAAAFF); // 연한 노란색
    llDebug:    Result := TColor($AAFFAA); // 연한 녹색
    llInfo:     Result := TColor($FFFFFF); // 흰색
    llWarning:  Result := TColor($55AAFF); // 주황색
    llError:    Result := TColor($5555FF); // 빨간색
    llFatal:    Result := TColor($0000FF); // 진한 빨간색
    else        Result := TColor($FFFFFF); // 기본 흰색
  end;
end;

procedure TPrintfHandler.FlushQueue;
var
  List: TList;
  i: Integer;
  LogItem: PPrintfLogQueueItem;
begin
  if not Assigned(FLogForm) then
    Exit;

  List := FLogQueue.LockList;
  try
    if List.Count = 0 then
      Exit;

    // 모든 큐 항목 처리
    for i := 0 to List.Count - 1 do
    begin
      LogItem := PPrintfLogQueueItem(List[i]);

      // 로그 추가 (소스 정보 포함)
      try
        FLogForm.AddLog(LogItem^.Message, LogItem^.Level, LogItem^.Source);
      except
        // 추가 실패 무시
      end;

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

procedure TPrintfHandler.ProcessLogQueue;
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

procedure TPrintfHandler.HandleFormClear(Sender: TObject);
begin
  // 로그 지우기 이벤트 처리
end;

procedure TPrintfHandler.HandleFormSave(Sender: TObject);
begin
  // 로그 저장 이벤트 처리
end;

procedure TPrintfHandler.SetFormPosition(Value: TFormPosition);
begin
  FFormPosition := Value;

  if not Assigned(FLogForm) then
    Exit;

  case Value of
    fpTopLeft:
      FLogForm.SetBounds(0, 0, FFormWidth, FFormHeight);

    fpBottomLeft:
      FLogForm.SetBounds(0, Screen.Height - FFormHeight - 40, FFormWidth, FFormHeight);

    fpTopRight:
      FLogForm.SetBounds(Screen.Width - FFormWidth, 0, FFormWidth, FFormHeight);

    fpBottomRight:
      FLogForm.SetBounds(Screen.Width - FFormWidth, Screen.Height - FFormHeight - 40, FFormWidth, FFormHeight);

    fpCustom:
      ; // 사용자 정의 위치는 변경하지 않음
  end;
end;

procedure TPrintfHandler.SetFormWidth(Value: Integer);
begin
  FFormWidth := Value;

  if Assigned(FLogForm) then
  begin
    FLogForm.Width := Value;
    SetFormPosition(FFormPosition); // 위치 재조정
  end;
end;

procedure TPrintfHandler.SetFormHeight(Value: Integer);
begin
  FFormHeight := Value;

  if Assigned(FLogForm) then
  begin
    FLogForm.Height := Value;
    SetFormPosition(FFormPosition); // 위치 재조정
  end;
end;

procedure TPrintfHandler.SetFormCaption(Value: string);
begin
  FFormCaption := Value;

  if Assigned(FLogForm) then
    FLogForm.Caption := Value;
end;

procedure TPrintfHandler.SetStayOnTop(Value: Boolean);
begin
  FStayOnTop := Value;

  if Assigned(FLogForm) then
  begin
    if Value then
    begin
      FLogForm.FormStyle := fsStayOnTop;
      FLogForm.StatusBar.Panels[0].Text := '윈도우 상태: 항상 위';
    end
    else
    begin
      FLogForm.FormStyle := fsNormal;
      FLogForm.StatusBar.Panels[0].Text := '윈도우 상태: 표준';
    end;
  end;
end;

procedure TPrintfHandler.SetVisible(Value: Boolean);
begin
  FVisible := Value;

  if Assigned(FLogForm) then
  begin
    if Value then
    begin
      FLogForm.Show;

      // 폼이 다시 표시될 때 트리 갱신
      FLogForm.LogTree.Invalidate;

      // 자동 스크롤이 활성화되어 있다면 마지막 항목으로 스크롤
      if FLogForm.TraceMode = tmAutoScroll then
      begin
        if FLogForm.LogTree.TotalCount > 0 then
          FLogForm.LogTree.ScrollIntoView(FLogForm.LogTree.GetLast, True);
      end;
    end
    else
      FLogForm.Hide;
  end;
end;

procedure TPrintfHandler.UpdateFormVisibility(Visiblity: Boolean);
begin
  FVisible := Visiblity;
end;

procedure TPrintfHandler.SetFormDesign(const ACaption: string; AWidth, AHeight: Integer;
                                     APosition: TFormPosition);
begin
  FFormCaption := ACaption;
  FFormWidth := AWidth;
  FFormHeight := AHeight;
  FFormPosition := APosition;

  if Assigned(FLogForm) then
  begin
    FLogForm.Caption := ACaption;
    FLogForm.Width := AWidth;
    FLogForm.Height := AHeight;
    SetFormPosition(APosition);
  end;
end;

procedure TPrintfHandler.ShowForm(Show: Boolean);
var
  i: Integer;
  MenuItem: TMenuItem;
begin
  SetVisible(Show);

  // 로그 폼 트레이 아이콘 메뉴 업데이트
  if Assigned(FLogForm) and Assigned(FLogForm.FTrayIcon) and
     Assigned(FLogForm.FTrayIcon.PopupMenu) and
     (FLogForm.FTrayIcon.PopupMenu.Items.Count > 0) then
  begin
    if Show then
      FLogForm.FTrayIcon.PopupMenu.Items[0].Caption := '로그 창 숨기기'
    else
      FLogForm.FTrayIcon.PopupMenu.Items[0].Caption := '로그 창 보이기';
  end;

  // 로그 폼의 팝업 메뉴에서 '로그 보이기' 메뉴 활성화/비활성화
  if Assigned(FLogForm) and Assigned(FLogForm.LogTree) and
     Assigned(FLogForm.LogTree.PopupMenu) then
  begin
    for i := 0 to FLogForm.LogTree.PopupMenu.Items.Count - 1 do
    begin
      MenuItem := FLogForm.LogTree.PopupMenu.Items[i];
      if MenuItem.Tag = 4 then // 로그 보이기 메뉴
      begin
        MenuItem.Enabled := not Show; // 폼이 보이면 비활성화, 숨겨져 있으면 활성화
        Break;
      end;
    end;
  end;
end;

procedure TPrintfHandler.SetFont(const FontName: string; FontSize: Integer; FontColor: TColor);
begin
  if Assigned(FLogForm) and Assigned(FLogForm.LogTree) then
  begin
    FLogForm.LogTree.Font.Name := FontName;
    FLogForm.LogTree.Font.Size := FontSize;
    FLogForm.LogTree.Font.Color := FontColor;

    // 노드 갱신을 위한 다시 그리기
    FLogForm.LogTree.Invalidate;
  end;
end;

procedure TPrintfHandler.SetBackgroundColor(Color: TColor);
begin
  if Assigned(FLogForm) and Assigned(FLogForm.LogTree) then
  begin
    FLogForm.LogTree.Color := Color;
    // 노드 갱신을 위한 다시 그리기
    FLogForm.LogTree.Invalidate;
  end;
end;

procedure TPrintfHandler.SaveToFile(const FileName: string);
begin
  if Assigned(FLogForm) then
    FLogForm.SaveLogs(FileName);
end;

procedure TPrintfHandler.ClearLog;
begin
  if Assigned(FLogForm) then
    FLogForm.ClearLogs;
end;

class procedure TPrintfHandler.OpenLogFile(AOwner: TComponent);
var
  OpenDialog: TOpenDialog;
  ViewerForm: TPrintfLogForm;
begin
  OpenDialog := TOpenDialog.Create(nil);
  try
    OpenDialog.Title := '로그 파일 열기';
    OpenDialog.Filter := '로그 파일 (*.log)|*.log|텍스트 파일 (*.txt)|*.txt|모든 파일 (*.*)|*.*';
    OpenDialog.InitialDir := ExtractFilePath(Application.ExeName);

    if OpenDialog.Execute then
    begin
      // 새 로그 뷰어 폼 생성
      ViewerForm := TPrintfLogForm.CreateForFileViewing(AOwner, OpenDialog.FileName);
      ViewerForm.Show;
    end;
  finally
    OpenDialog.Free;
  end;
end;

end.
