unit LogViewerForm;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, Forms, Controls, Graphics, Dialogs, StdCtrls, ExtCtrls,
  DBGrids, DB, Buttons, ComCtrls, Menus, Clipbrd, CheckLst, Spin,
  LCLType,
  ZConnection, ZDataset, ZSqlUpdate, RxDBGrid, RxSortZeos, RxDBGridExportSpreadSheet,
  DbCtrls, DateUtils, DateTimePicker,
  DatabaseHandler;

// 이 전역 변수를 추가합니다 - 리소스 없이 파생된 폼을 만들 수 있게 합니다
{$IFDEF FPC}
  {$PUSH}
  {$WARN 5024 OFF : Parameter "$1" not used}
{$ENDIF}
var
  RequireDerivedFormResource: Boolean = False;
{$IFDEF FPC}
  {$POP}
{$ENDIF}

type
  // 검색 기록을 저장하기 위한 레코드 타입
  TSearchHistory = record
    SearchText: string;
    LogLevel: Integer;
    DateFrom: TDateTime;
    DateTo: TDateTime;
  end;


  // 로그 뷰어 폼 클래스
  { TfrmLogViewer }
  TfrmLogViewer = class(TForm)
  private
    // UI 컴포넌트
    btnRefresh: TButton;
    btnClearLog: TButton;
    btnSearchClear: TButton;
    btnAutoRefresh: TSpeedButton;
    btnSaveSearch: TButton;
    btnLoadSearch: TButton;
    cbLogLevel: TComboBox;
    clbLogLevels: TCheckListBox;
    chkAutoScroll: TCheckBox;
    chkMultiLevel: TCheckBox;
    DataSource: TDataSource;
    DateFrom: TDateTimePicker;
    DateTo: TDateTimePicker;
    edtSearch: TEdit;
    edtRefreshInterval: TSpinEdit;
    gbFilter: TGroupBox;
    gbRefresh: TGroupBox;
    lblCount: TLabel;
    lblDate: TLabel;
    lblDateTo: TLabel;
    lblLevel: TLabel;
    lblSearch: TLabel;
    lblRefreshInterval: TLabel;
    LogConnection: TZConnection;
    LogQuery: TZQuery;
    MainMenu: TMainMenu;
    MenuItem1: TMenuItem;
    miExportCSV: TMenuItem;
    miExportXLS: TMenuItem;
    miExit: TMenuItem;
    miDetailLog: TMenuItem;
    miFilteredDelete: TMenuItem;
    Panel1: TPanel;
    Panel2: TPanel;
    RxDBGrid: TRxDBGrid;
    RxDBGridExportSpreadSheet: TRxDBGridExportSpreadSheet;
    SaveDialog: TSaveDialog;
    TimerRefresh: TTimer;
    miFindDlg: TMenuItem;
    miColumnsDlg: TMenuItem;
    miFilterDlg: TMenuItem;
    miSortDlg: TMenuItem;
    miOptimizeColumns: TMenuItem;
    N1: TMenuItem;
    N2: TMenuItem;

    // 레벨 필터 체크박스들
    chkINFO: TCheckBox;
    chkDEBUG: TCheckBox;
    chkWARN: TCheckBox;
    chkERROR: TCheckBox;
    chkFATAL: TCheckBox;
    chkDEVEL: TCheckBox;

    // 비공개 필드
    FAutoRefresh: Boolean;
    FLastPosition: Integer;
    FClearingFilter: Boolean;
    FSearchHistory: array of TSearchHistory;
    FLoading: Boolean;

    FDBHandler: TDatabaseHandler;

    // UI 컴포넌트 초기화 및 설정
    procedure CreateComponents;
    procedure SetupGrid;
    procedure SetupPopupMenu;
    procedure ApplyTheme;

    // 데이터 관련 메소드
    procedure LogLevelCheckClick(Sender: TObject);
    procedure RefreshData;
    procedure ApplyFilter;
    procedure TryApplyFilterWithSQL(const WhereClause: string);
    procedure ExportToCSV(const FileName: string);
    procedure ExportToXLS(const FileName: string);
    procedure UpdateRowCount;
    procedure LoadSearchHistory;
    procedure SaveSearchHistory;

    // 이벤트 핸들러
    procedure btnAutoRefreshClick(Sender: TObject);
    procedure btnClearLogClick(Sender: TObject);
    procedure btnRefreshClick(Sender: TObject);
    procedure btnSearchClearClick(Sender: TObject);
    procedure btnSaveSearchClick(Sender: TObject);
    procedure btnLoadSearchClick(Sender: TObject);
    procedure cbLogLevelChange(Sender: TObject);
    procedure chkMultiLevelClick(Sender: TObject);
    procedure clbLogLevelsClickCheck(Sender: TObject);
    procedure DateFromChange(Sender: TObject);
    procedure DateToChange(Sender: TObject);
    procedure edtRefreshIntervalChange(Sender: TObject);
    procedure edtSearchChange(Sender: TObject);
    procedure FormClose(Sender: TObject; var CloseAction: TCloseAction);
    procedure FormShow(Sender: TObject);
    procedure FormKeyDown(Sender: TObject; var Key: Word; Shift: TShiftState);
    procedure miExportCSVClick(Sender: TObject);
    procedure miExportXLSClick(Sender: TObject);
    procedure miExitClick(Sender: TObject);
    procedure miDetailLogClick(Sender: TObject);
    procedure miFilteredDeleteClick(Sender: TObject);
    procedure RxDBGridGetCellProps(Sender: TObject; Field: TField;
      AFont: TFont; var Background: TColor);
    procedure RxDBGridDblClick(Sender: TObject);
    procedure TimerRefreshTimer(Sender: TObject);

    // 팝업 메뉴 이벤트 핸들러
    procedure miFindDlgClick(Sender: TObject);
    procedure miColumnsDlgClick(Sender: TObject);
    procedure miFilterDlgClick(Sender: TObject);
    procedure miSortDlgClick(Sender: TObject);
    procedure miOptimizeColumnsClick(Sender: TObject);

    // 유틸리티 메소드
    procedure ShowDetailLog;
    procedure DeleteFilteredLogs;

  public
    constructor Create(AOwner: TComponent); override;
    constructor CreateWithDBHandler(AOwner: TComponent; ADBHandler: TDatabaseHandler);
    destructor Destroy; override;

    property DBHandler: TDatabaseHandler read FDBHandler write FDBHandler;
  end;

var
  frmLogViewer: TfrmLogViewer;


  function SelectFromList(const Title, Prompt: string; Items: TStrings): Integer;

implementation

//{$R *.lfm}

uses
  GlobalLogger, LogHandlers, StrUtils, IniFiles;

{ TfrmLogViewer }

constructor TfrmLogViewer.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);

  FLoading := True;
  try
    // UI 컴포넌트 생성 및 설정
    CreateComponents;

    // 폼 설정
    Caption := '로그 뷰어';
    Width := 820;  // 더 넓게 설정
    Height := 550; // 더 높게 설정
    Position := poScreenCenter;
    KeyPreview := True;  // 키 이벤트를 폼에서 처리

    OnClose := @FormClose;
    OnShow := @FormShow;
    OnKeyDown := @FormKeyDown;

    // 그리드 설정
    SetupGrid;
    // PopupMenu 설정
    SetupPopupMenu;

    // DB 연결 설정
    LogConnection.Protocol := 'firebird';
    LogConnection.ClientCodepage := 'UTF8';
    LogConnection.LibraryLocation := ExtractFilePath(ParamStr(0)) + 'fbclient.dll';
    LogConnection.Database := ExtractFilePath(ParamStr(0)) + 'logs\Log.fdb';
    LogConnection.User := 'SYSDBA';
    LogConnection.Password := 'masterkey';

    try
      LogConnection.Connect;
    except
      on E: Exception do
      begin
        ShowMessage('로그 데이터베이스 연결 오류: ' + E.Message);
      end;
    end;

    // 타이머 설정
    FAutoRefresh := False;
    btnAutoRefresh.Down := False;
    TimerRefresh.Enabled := False;
    TimerRefresh.Interval := StrToIntDef(edtRefreshInterval.Text, 5) * 1000;

    // 검색 기록 로드
    LoadSearchHistory;
  finally
    FLoading := False;
  end;
end;

// 생성자 구현 추가
constructor TfrmLogViewer.CreateWithDBHandler(AOwner: TComponent; ADBHandler: TDatabaseHandler);
begin
  Create(AOwner);
  FDBHandler := ADBHandler;
  RefreshData;
end;

destructor TfrmLogViewer.Destroy;
begin
  try
    // 타이머 비활성화
    TimerRefresh.Enabled := False;

    // 데이터베이스 연결 닫기
    if LogQuery <> nil then
      LogQuery.Close;
    if LogConnection <> nil then
      LogConnection.Disconnect;

    // 검색 기록 저장
    SaveSearchHistory;
  except
    // 소멸자에서 예외가 발생하더라도 무시하고 진행
    on E: Exception do
      ; // 아무 작업 안함
  end;

  inherited Destroy;
end;

procedure TfrmLogViewer.CreateComponents;
begin
  try
    // === 패널 및 그룹박스 ===
    Panel1 := TPanel.Create(Self);
    Panel1.Parent := Self;
    Panel1.Align := alTop;
    Panel1.Height := 116;
    Panel1.BevelOuter := bvNone;

    Panel2 := TPanel.Create(Self);
    Panel2.Parent := Self;
    Panel2.Align := alBottom;
    Panel2.Height := 25;
    Panel2.BevelOuter := bvNone;
    Panel2.Font.Name:= 'Arial';

    gbFilter := TGroupBox.Create(Self);
    gbFilter.Parent := Panel1;
    gbFilter.Caption := '필터';
    gbFilter.Left := 8;
    gbFilter.Top := 8;
    gbFilter.Width := 518;
    gbFilter.Height := 106;

    gbRefresh := TGroupBox.Create(Self);
    gbRefresh.Parent := Panel1;
    gbRefresh.Caption := '새로고침 설정';
    gbRefresh.Left := 534;
    gbRefresh.Top := 8;
    gbRefresh.Width := 270;
    gbRefresh.Height := 106;

    // === 체크박스 (레벨 필터) - 첫 번째 행에 배치 ===
    chkINFO := TCheckBox.Create(Self);
    chkINFO.Parent := gbFilter;
    chkINFO.Caption := 'INFO';
    chkINFO.Left := 11;
    chkINFO.Top := 8;
    chkINFO.Width := 48;
    chkINFO.Checked := True;
    chkINFO.OnClick := @LogLevelCheckClick;

    chkDEBUG := TCheckBox.Create(Self);
    chkDEBUG.Parent := gbFilter;
    chkDEBUG.Caption := 'DEBUG';
    chkDEBUG.Left := 71;
    chkDEBUG.Top := 8;
    chkDEBUG.Width := 58;
    chkDEBUG.Checked := True;
    chkDEBUG.OnClick := @LogLevelCheckClick;

    chkWARN := TCheckBox.Create(Self);
    chkWARN.Parent := gbFilter;
    chkWARN.Caption := 'WARN';
    chkWARN.Left := 141;
    chkWARN.Top := 8;
    chkWARN.Width := 53;
    chkWARN.Checked := True;
    chkWARN.OnClick := @LogLevelCheckClick;

    chkERROR := TCheckBox.Create(Self);
    chkERROR.Parent := gbFilter;
    chkERROR.Caption := 'ERROR';
    chkERROR.Left := 201;
    chkERROR.Top := 8;
    chkERROR.Width := 60;
    chkERROR.Checked := True;
    chkERROR.OnClick := @LogLevelCheckClick;

    chkFATAL := TCheckBox.Create(Self);
    chkFATAL.Parent := gbFilter;
    chkFATAL.Caption := 'FATAL';
    chkFATAL.Left := 266;
    chkFATAL.Top := 8;
    chkFATAL.Width := 60;
    chkFATAL.Checked := True;
    chkFATAL.OnClick := @LogLevelCheckClick;

    chkDEVEL := TCheckBox.Create(Self);
    chkDEVEL.Parent := gbFilter;
    chkDEVEL.Caption := 'DEVEL';
    chkDEVEL.Left := 331;
    chkDEVEL.Top := 8;
    chkDEVEL.Width := 60;
    chkDEVEL.Checked := True;
    chkDEVEL.OnClick := @LogLevelCheckClick;

    // === 레이블 ===
    lblDate := TLabel.Create(Self);
    lblDate.Parent := gbFilter;
    lblDate.Caption := '시작일';
    lblDate.Left := 11;
    lblDate.Top := 38;

    lblDateTo := TLabel.Create(Self);
    lblDateTo.Parent := gbFilter;
    lblDateTo.Caption := '종료일';
    lblDateTo.Left := 104;
    lblDateTo.Top := 38;

    lblSearch := TLabel.Create(Self);
    lblSearch.Parent := gbFilter;
    lblSearch.Caption := '검색어';
    lblSearch.Left := 210;
    lblSearch.Top := 38;

    lblCount := TLabel.Create(Self);
    lblCount.Parent := Panel2;
    lblCount.Caption := 'ShowQuickFilter:(Ctrl+Shift+Q)   HideQuickFilter:(Ctrl+Shift+H)';
    lblCount.Left := 8;
    lblCount.Top := 5;

    lblRefreshInterval := TLabel.Create(Self);
    lblRefreshInterval.Parent := gbRefresh;
    lblRefreshInterval.Caption := '새로고침 간격(초):';
    lblRefreshInterval.Left := 10;
    lblRefreshInterval.Top := 8;

    // 날짜 선택기 설정
    DateFrom := TDateTimePicker.Create(Self);
    DateFrom.Parent := gbFilter;
    DateFrom.Left := 11;
    DateFrom.Top := 58;
    DateFrom.Width := 83;
    DateFrom.Date := Date - 7; // 기본값으로 1주일 전
    DateFrom.OnChange := @DateFromChange;

    DateTo := TDateTimePicker.Create(Self);
    DateTo.Parent := gbFilter;
    DateTo.Left := 104;
    DateTo.Top := 58;
    DateTo.Width := 83;
    DateTo.Date := Date;
    DateTo.OnChange := @DateToChange;

    // 검색창 설정
    edtSearch := TEdit.Create(Self);
    edtSearch.Parent := gbFilter;
    edtSearch.Left := 210;
    edtSearch.Top := 58;
    edtSearch.Width := 200;
    edtSearch.OnChange := @edtSearchChange;

    // 검색 관련 버튼 - 垂直 배치
    btnLoadSearch := TButton.Create(Self);
    btnLoadSearch.Parent := gbFilter;
    btnLoadSearch.Caption := '검색 불러오기';
    btnLoadSearch.Left := 419;
    btnLoadSearch.Top := 6;
    btnLoadSearch.Width := 88;
    btnLoadSearch.Height := 23;
    btnLoadSearch.OnClick := @btnLoadSearchClick;

    btnSaveSearch := TButton.Create(Self);
    btnSaveSearch.Parent := gbFilter;
    btnSaveSearch.Caption := '검색 저장';
    btnSaveSearch.Left := 419;
    btnSaveSearch.Top := 32;
    btnSaveSearch.Width := 88;
    btnSaveSearch.Height := 23;
    btnSaveSearch.OnClick := @btnSaveSearchClick;

    btnSearchClear := TButton.Create(Self);
    btnSearchClear.Parent := gbFilter;
    btnSearchClear.Caption := '지우기';
    btnSearchClear.Left := 419;
    btnSearchClear.Top := 58;
    btnSearchClear.Width := 88;
    btnSearchClear.Height := 23;
    btnSearchClear.OnClick := @btnSearchClearClick;

    // 자동 스크롤 체크박스
    chkAutoScroll := TCheckBox.Create(Self);
    chkAutoScroll.Parent := gbRefresh;
    chkAutoScroll.Caption := '최신 로그를 상단에 표시';
    chkAutoScroll.Left := 10;
    chkAutoScroll.Top := 32;
    chkAutoScroll.Width := 180;
    chkAutoScroll.Checked := True;

    // 새로고침 간격 설정
    edtRefreshInterval := TSpinEdit.Create(Self);
    edtRefreshInterval.Parent := gbRefresh;
    edtRefreshInterval.Left := 120;
    edtRefreshInterval.Top := 6;
    edtRefreshInterval.Width := 60;
    edtRefreshInterval.MinValue := 1;
    edtRefreshInterval.MaxValue := 60;
    edtRefreshInterval.Value := 5;
    edtRefreshInterval.OnChange := @edtRefreshIntervalChange;

    // === 버튼 설정 ===
    btnRefresh := TButton.Create(Self);
    btnRefresh.Parent := gbRefresh;
    btnRefresh.Caption := '새로고침';
    btnRefresh.Left := 10;
    btnRefresh.Top := 58;
    btnRefresh.Width := 70;
    btnRefresh.Height := 25;
    btnRefresh.OnClick := @btnRefreshClick;

    btnAutoRefresh := TSpeedButton.Create(Self);
    btnAutoRefresh.Parent := gbRefresh;
    btnAutoRefresh.Caption := '자동새로고침';
    btnAutoRefresh.Left := 85;
    btnAutoRefresh.Top := 58;
    btnAutoRefresh.Width := 90;
    btnAutoRefresh.Height := 25;
    btnAutoRefresh.AllowAllUp := True;
    btnAutoRefresh.GroupIndex := 1;
    btnAutoRefresh.OnClick := @btnAutoRefreshClick;

    btnClearLog := TButton.Create(Self);
    btnClearLog.Parent := gbRefresh;
    btnClearLog.Caption := '로그 지우기';
    btnClearLog.Left := 180;
    btnClearLog.Top := 58;
    btnClearLog.Width := 80;
    btnClearLog.Height := 25;
    btnClearLog.OnClick := @btnClearLogClick;

    // === 데이터 컴포넌트 ===
    RxDBGrid := TRxDBGrid.Create(Self);
    RxDBGrid.Parent := Self;
    RxDBGrid.Align := alClient;
    RxDBGrid.OnGetCellProps := @RxDBGridGetCellProps;
    RxDBGrid.OnDblClick := @RxDBGridDblClick;
    RxDBGrid.TitleButtons := True;
    RxDBGrid.ReadOnly := True;
    RxDBGrid.FooterOptions.Active := True;
    RxDBGrid.DrawFullLine := True;

    RxDBGridExportSpreadSheet := TRxDBGridExportSpreadSheet.Create(Self);

    LogConnection := TZConnection.Create(Self);
    LogQuery := TZQuery.Create(Self);
    DataSource := TDataSource.Create(Self);

    // === 메뉴 및 다이얼로그 ===
    MainMenu := TMainMenu.Create(Self);
    PopupMenu := TPopupMenu.Create(Self);
    RxDBGrid.PopupMenu := PopupMenu;

    MenuItem1 := TMenuItem.Create(Self);
    MenuItem1.Caption := '파일';
    MainMenu.Items.Add(MenuItem1);

    miExportCSV := TMenuItem.Create(Self);
    miExportCSV.Caption := 'CSV 내보내기...';
    miExportCSV.OnClick := @miExportCSVClick;
    MenuItem1.Add(miExportCSV);

    miExportXLS := TMenuItem.Create(Self);
    miExportXLS.Caption := 'Excel 내보내기...';
    miExportXLS.OnClick := @miExportXLSClick;
    MenuItem1.Add(miExportXLS);

    N2 := TMenuItem.Create(Self);
    N2.Caption := '-';
    MenuItem1.Add(N2);

    miDetailLog := TMenuItem.Create(Self);
    miDetailLog.Caption := '로그 상세 보기';
    miDetailLog.ShortCut := ShortCut(VK_F3, []);
    miDetailLog.OnClick := @miDetailLogClick;
    MenuItem1.Add(miDetailLog);

    miFilteredDelete := TMenuItem.Create(Self);
    miFilteredDelete.Caption := '필터된 로그 삭제';
    miFilteredDelete.OnClick := @miFilteredDeleteClick;
    MenuItem1.Add(miFilteredDelete);

    N1 := TMenuItem.Create(Self);
    N1.Caption := '-';
    MenuItem1.Add(N1);

    miExit := TMenuItem.Create(Self);
    miExit.Caption := '종료';
    miExit.OnClick := @miExitClick;
    MenuItem1.Add(miExit);

    SaveDialog := TSaveDialog.Create(Self);

    TimerRefresh := TTimer.Create(Self);
    TimerRefresh.Enabled := False;
    TimerRefresh.OnTimer := @TimerRefreshTimer;

    // 데이터 연결 설정
    LogQuery.Connection := LogConnection;
    DataSource.DataSet := LogQuery;
    RxDBGrid.DataSource := DataSource;

    // 메뉴 설정
    Menu := MainMenu;
  except
    on E: Exception do
      ShowMessage('컴포넌트 초기화 중 오류가 발생했습니다: ' + E.Message);
  end;
end;

procedure TfrmLogViewer.SetupGrid;
var
  Column: TRxColumn;
begin
  with RxDBGrid do
  begin
    // 기존 열 모두 제거
    Columns.Clear;

    // ID 열 추가 (숨김)
    Column := TRxColumn.Create(Columns);
    Column.FieldName := 'ID';
    Column.Visible := False;

    // LDATE 열 추가
    Column := TRxColumn.Create(Columns);
    Column.FieldName := 'LDATE';
    Column.Alignment := taCenter;
    Column.Filter.AllValue := '*';
    Column.Title.Caption := '일자';
    Column.Title.Alignment := taCenter;
    Column.Title.Font.Style := [fsBold];
    Column.Width := 74;

    // LTIME 열 추가
    Column := TRxColumn.Create(Columns);
    Column.FieldName := 'LTIME';
    Column.Alignment := taCenter;
    Column.Filter.AllValue := '*';
    Column.Filter.Style := rxfstmanualEdit;
    Column.DisplayFormat := 'hh:nn:ss.zzz';
    Column.Title.Caption := '시간';
    Column.Title.Alignment := taCenter;
    Column.Title.Font.Style := [fsBold];
    Column.Width := 80;

    // LLEVEL 열 추가
    Column := TRxColumn.Create(Columns);
    Column.FieldName := 'LLEVEL';
    Column.Filter.AllValue := '*';
    Column.Title.Caption := '레벨';
    Column.Title.Alignment := taCenter;
    Column.Title.Font.Style := [fsBold];
    Column.Width := 70;

    // LSOURCE 열 추가
    Column := TRxColumn.Create(Columns);
    Column.FieldName := 'LSOURCE';
    Column.Filter.AllValue := '*';
    Column.Title.Caption := '소스';
    Column.Title.Alignment := taCenter;
    Column.Title.Font.Style := [fsBold];
    Column.Width := 90;

    // LMESSAGE 열 추가
    Column := TRxColumn.Create(Columns);
    Column.FieldName := 'LMESSAGE';
    Column.Filter.AllValue := '*';
    Column.Filter.Style := rxfstmanualEdit;
    Column.Title.Caption := '메시지';
    Column.Title.Alignment := taCenter;
    Column.Title.Font.Style := [fsBold];
    Column.Width := 400;

    // 기본 그리드 설정
    Options := [dgTitles, dgIndicator, dgColumnResize, dgColumnMove, dgColLines,
                dgRowLines, dgAlwaysShowSelection, dgConfirmDelete, dgCancelOnExit,
                dgHeaderHotTracking, dgHeaderPushedLook, dgAnyButtonCanSelect,
                dgDisableDelete, dgDisableInsert, dgTruncCellHints, dgThumbTracking,
                dgDisplayMemoText, dgRowSelect, dgMultiselect];

    OptionsRx := [rdgAllowFilterForm, rdgAllowColumnsForm, rdgAllowDialogFind,
                  rdgAllowQuickFilter, rdgAllowQuickSearch, rdgAllowSortForm,
                  rdgAllowToolMenu, rdgFooterRows, rdgHighlightFocusCol, rdgMultiTitleLines];

    // 단축키 설정
    KeyStrokes[4].ShortCut := ShortCut(Ord('Q'), [ssCtrl, ssShift]);
    KeyStrokes[5].ShortCut := ShortCut(Ord('H'), [ssCtrl, ssShift]);

    // 확장 옵션 설정
    TitleButtons := True;  // 헤더 클릭으로 정렬

    // 푸터에 로그 개수 표시
    FooterOptions.Active := True;
    FooterOptions.RowCount := 1;
    Columns[1].Footer.ValueType := fvtCount;
    Columns[1].Footer.FieldName := 'LDATE';
  end;
end;

procedure TfrmLogViewer.SetupPopupMenu;
begin
  // 팝업 메뉴 항목 추가
  miFindDlg := TMenuItem.Create(PopupMenu);
  miFindDlg.Caption := '검색 대화상자 (Ctrl+F)';
  miFindDlg.ShortCut := ShortCut(Ord('F'), [ssCtrl]);
  miFindDlg.OnClick := @miFindDlgClick;
  PopupMenu.Items.Add(miFindDlg);

  miColumnsDlg := TMenuItem.Create(PopupMenu);
  miColumnsDlg.Caption := '열 설정 대화상자 (Ctrl+W)';
  miColumnsDlg.ShortCut := ShortCut(Ord('W'), [ssCtrl]);
  miColumnsDlg.OnClick := @miColumnsDlgClick;
  PopupMenu.Items.Add(miColumnsDlg);

  miFilterDlg := TMenuItem.Create(PopupMenu);
  miFilterDlg.Caption := '필터 대화상자 (Ctrl+T)';
  miFilterDlg.ShortCut := ShortCut(Ord('T'), [ssCtrl]);
  miFilterDlg.OnClick := @miFilterDlgClick;
  PopupMenu.Items.Add(miFilterDlg);

  miSortDlg := TMenuItem.Create(PopupMenu);
  miSortDlg.Caption := '정렬 대화상자 (Ctrl+S)';
  miSortDlg.ShortCut := ShortCut(Ord('S'), [ssCtrl]);
  miSortDlg.OnClick := @miSortDlgClick;
  PopupMenu.Items.Add(miSortDlg);

  miOptimizeColumns := TMenuItem.Create(PopupMenu);
  miOptimizeColumns.Caption := '컬럼 너비 최적화';
  miOptimizeColumns.OnClick := @miOptimizeColumnsClick;
  PopupMenu.Items.Add(miOptimizeColumns);

  // 구분선 추가
  N1 := TMenuItem.Create(PopupMenu);
  N1.Caption := '-';
  PopupMenu.Items.Add(N1);

  miExportXLS := TMenuItem.Create(PopupMenu);
  miExportXLS.Caption := 'Excel 내보내기 (Ctrl+X)';
  miExportXLS.ShortCut := ShortCut(Ord('X'), [ssCtrl]);
  miExportXLS.OnClick := @miExportXLSClick;
  PopupMenu.Items.Add(miExportXLS);
end;

procedure TfrmLogViewer.ApplyTheme;
begin
  // UI 테마 적용 (사용자 시스템 설정에 맞게 조정 가능)
  Color := clBtnFace;
  Font.Name := 'Tahoma';
  Font.Size := 9;

  // 그리드 색상 설정
  RxDBGrid.Color := clWindow;
  RxDBGrid.FooterOptions.Color := clBtnFace;
  RxDBGrid.TitleFont.Style := [fsBold];
end;


procedure TfrmLogViewer.FormShow(Sender: TObject);
begin
  RefreshData;
  Self.KeyPreview := True;
end;

procedure TfrmLogViewer.FormKeyDown(Sender: TObject; var Key: Word; Shift: TShiftState);
begin
  // F3 키: 로그 상세 보기
  if Key = VK_F3 then
  begin
    ShowDetailLog;
    Key := 0; // 키 이벤트 소비
  end;
end;

procedure TfrmLogViewer.LogLevelCheckClick(Sender: TObject);
begin
  if not FClearingFilter then
    ApplyFilter;
end;

procedure TfrmLogViewer.RefreshData;
var
  LastID: Integer;
  ScrollToEnd: Boolean;
  SavedCursor: TCursor;
  TempQuery: TZQuery;
  SearchText: string;
begin
  SavedCursor := Screen.Cursor;
  Screen.Cursor := crHourGlass;  // 모래시계 커서 표시

  try
    // 현재 선택된 레코드의 ID 저장
    FLastPosition := -1;
    if not LogQuery.IsEmpty then
    begin
      FLastPosition := LogQuery.FieldByName('ID').AsInteger;
    end;

    // 자동 스크롤 설정 확인
    ScrollToEnd := chkAutoScroll.Checked;

    if not Assigned(FDBHandler) then // 수정
    begin
      // DatabaseHandler가 없으면
      DebugToFile('TfrmLogViewer.RefreshData: FDBHandler가 할당되지 않았습니다.');
    end
    else
    begin
      // 검색어
      SearchText := Trim(edtSearch.Text);

      // DatabaseHandler의 GetLogs 메서드 사용하여 로그 조회
      TempQuery := FDBHandler.GetLogs(
        DateFrom.Date,       // 시작일
        DateTo.Date,         // 종료일
        '',                  // 로그 레벨 필터 (비워두고 UI에서 처리)
        SearchText           // 검색어
      );

      // 기존 쿼리 닫기
      if LogQuery.Active then
        LogQuery.Close;

      // 쿼리 복사
      LogQuery.SQL.Text := TempQuery.SQL.Text;
      LogQuery.Params.Assign(TempQuery.Params);
      LogQuery.Open;

      // 임시 쿼리 해제
      TempQuery.Close;
      TempQuery.Free;
    end;

    // 로그 레벨 필터 적용
    ApplyFilter;

    // 마지막 위치로 이동 또는 맨 끝으로 이동
    if ScrollToEnd and (not LogQuery.IsEmpty) then
    begin
      LogQuery.First;
    end
    else if FLastPosition > 0 then
    begin
      LogQuery.Locate('ID', FLastPosition, []);
    end;

    // 로그 레코드 카운트 업데이트
    UpdateRowCount;
  finally
    Screen.Cursor := SavedCursor;  // 원래 커서로 복원
  end;
end;

procedure TfrmLogViewer.ApplyFilter;
var
  FilterStr: string;
  SelectedLevels: TStringList;
  i: Integer;
begin
  FClearingFilter := True;
  try
    // 로그 레벨 필터 - 체크박스 방식
    SelectedLevels := TStringList.Create;
    try
      if chkINFO.Checked then
        SelectedLevels.Add('INFO');
      if chkDEBUG.Checked then
        SelectedLevels.Add('DEBUG');
      if chkWARN.Checked then
        SelectedLevels.Add('WARNING');
      if chkERROR.Checked then
        SelectedLevels.Add('ERROR');
      if chkFATAL.Checked then
        SelectedLevels.Add('FATAL');
      if chkDEVEL.Checked then
        SelectedLevels.Add('DEVEL');

      // 모든 로그 레벨이 선택된 경우 필터 해제
      if SelectedLevels.Count = 6 then
      begin
        LogQuery.Filtered := False;
      end
      else if SelectedLevels.Count > 0 then
      begin
        // 일부 로그 레벨만 선택된 경우
        FilterStr := '';
        for i := 0 to SelectedLevels.Count - 1 do
        begin
          if i > 0 then
            FilterStr := FilterStr + ' OR ';
          FilterStr := FilterStr + Format('LLEVEL = ''%s''', [SelectedLevels[i]]);
        end;

        // 필터 적용
        LogQuery.Filter := '(' + FilterStr + ')';
        LogQuery.Filtered := True;
      end
      else
      begin
        // 어떤 로그 레벨도 선택되지 않은 경우 (모든 로그 숨김)
        LogQuery.Filter := 'LLEVEL = ''NONE''';
        LogQuery.Filtered := True;
      end;
    finally
      SelectedLevels.Free;
    end;
  finally
    FClearingFilter := False;
    UpdateRowCount;
  end;
end;

procedure TfrmLogViewer.TryApplyFilterWithSQL(const WhereClause: string);
begin
  // 월별 테이블 구조에서는 이 메서드는 더 이상 사용되지 않음
  // GetDatabaseHandler.GetLogs 메서드가 대신 사용됨

  // 필터를 SQL WHERE 절로 적용
  try
    LogQuery.Close;

    if WhereClause <> '' then
      LogQuery.SQL.Text :=
        'SELECT ID, LDATE, LTIME, LLEVEL, LSOURCE, LMESSAGE ' +
        'FROM LOGS ' +
        'WHERE ' + WhereClause + ' ' +
        'ORDER BY ID DESC'
    else
      LogQuery.SQL.Text :=
        'SELECT ID, LDATE, LTIME, LLEVEL, LSOURCE, LMESSAGE ' +
        'FROM LOGS ' +
        'ORDER BY ID DESC';

    LogQuery.Open;
  except
    on E: Exception do
    begin
      ShowMessage('필터 적용 중 오류가 발생했습니다: ' + E.Message);

      // 검색어 필터 없이 재시도
      try
        LogQuery.Close;
        LogQuery.SQL.Text :=
          'SELECT ID, LDATE, LTIME, LLEVEL, LSOURCE, LMESSAGE ' +
          'FROM LOGS ' +
          'ORDER BY ID DESC';
        LogQuery.Open;
      except
        on E2: Exception do
          ShowMessage('기본 쿼리 실행 중 오류: ' + E2.Message);
      end;
    end;
  end;
end;

procedure TfrmLogViewer.RxDBGridGetCellProps(Sender: TObject; Field: TField;
  AFont: TFont; var Background: TColor);
var
  Level: string;
begin
  if Field = nil then Exit;

  // 레벨에 따른 색상 설정
  if LogQuery.FieldByName('LLEVEL') <> nil then
  begin
    Level := LogQuery.FieldByName('LLEVEL').AsString;

    if Level = 'TRACE' then
      AFont.Color := clGray
    else if Level = 'DEBUG' then
      AFont.Color := clBlue
    else if Level = 'INFO' then
      AFont.Color := clBlack
    else if Level = 'WARNING' then
    begin
      AFont.Color := clMaroon;
      AFont.Style := [fsBold];
    end
    else if Level = 'ERROR' then
    begin
      AFont.Color := clRed;
      AFont.Style := [fsBold];
    end
    else if Level = 'FATAL' then
    begin
      AFont.Color := clRed;
      AFont.Style := [fsBold];
      Background := clYellow;
    end;
  end;
end;

procedure TfrmLogViewer.RxDBGridDblClick(Sender: TObject);
begin
  // 그리드 더블 클릭 시 로그 상세 보기
  ShowDetailLog;
end;

procedure TfrmLogViewer.btnRefreshClick(Sender: TObject);
begin
  RefreshData;
end;

procedure TfrmLogViewer.btnClearLogClick(Sender: TObject);
var
  LogTables: TStringList;
  i: Integer;
begin
  if MessageDlg('경고', '모든 로그 기록을 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.',
     mtWarning, [mbYes, mbNo], 0) = mrYes then
  begin
    try
      // DatabaseHandler 얻기
      if not Assigned(FDBHandler) then
      begin
        // 기존 단일 테이블 방식
        LogQuery.Close;
        LogQuery.SQL.Text := 'DELETE FROM LOGS';
        LogQuery.ExecSQL;
      end
      else
      begin
        // 월별 테이블 방식
        LogTables := TStringList.Create;
        try
          LogQuery.Close;

          // 메타 테이블 비우기
          LogQuery.SQL.Text := 'DELETE FROM ' + FDBHandler.MetaTableName;
          LogQuery.ExecSQL;

          // 테이블 목록 조회
          LogQuery.SQL.Text :=
            'SELECT RDB$RELATION_NAME FROM RDB$RELATIONS ' +
            'WHERE RDB$RELATION_NAME LIKE ''LOGS_%'' AND RDB$SYSTEM_FLAG = 0';
          LogQuery.Open;

          while not LogQuery.EOF do
          begin
            LogTables.Add(Trim(LogQuery.FieldByName('RDB$RELATION_NAME').AsString));
            LogQuery.Next;
          end;

          LogQuery.Close;

          // 모든 로그 테이블 삭제
          for i := 0 to LogTables.Count - 1 do
          begin
            try
              // 시퀀스 삭제
              LogQuery.SQL.Text := 'DROP SEQUENCE SEQ_' + LogTables[i];
              LogQuery.ExecSQL;

              // 테이블 삭제
              LogQuery.SQL.Text := 'DROP TABLE ' + LogTables[i];
              LogQuery.ExecSQL;
            except
              // 테이블 삭제 중 오류는 무시하고 계속 진행
              on E: Exception do
                ; // 무시
            end;
          end;
        finally
          LogTables.Free;
        end;

        // 현재 월 테이블 재생성 (DBHandler 초기화)
        FDBHandler.Init;
      end;

      RefreshData;
      ShowMessage('모든 로그가 삭제되었습니다.');
    except
      on E: Exception do
        ShowMessage('로그 삭제 중 오류가 발생했습니다: ' + E.Message);
    end;
  end;
end;

procedure TfrmLogViewer.btnAutoRefreshClick(Sender: TObject);
begin
  FAutoRefresh := btnAutoRefresh.Down;
  TimerRefresh.Enabled := FAutoRefresh;

  if FAutoRefresh then
    btnAutoRefresh.Caption := '자동 갱신 중...'
  else
    btnAutoRefresh.Caption := '자동 새로고침';
end;

procedure TfrmLogViewer.btnSearchClearClick(Sender: TObject);
begin
  edtSearch.Clear;

  // 체크박스 초기화 - 모두 선택
  chkINFO.Checked := True;
  chkDEBUG.Checked := True;
  chkWARN.Checked := True;
  chkERROR.Checked := True;
  chkFATAL.Checked := True;
  chkDEVEL.Checked := True;

  DateFrom.Date := Date - 7;
  DateTo.Date := Date;

  // 데이터 새로고침
  RefreshData;
end;

procedure TfrmLogViewer.btnSaveSearchClick(Sender: TObject);
var
  SearchName: string;
  NewSearch: TSearchHistory;
begin
  SearchName := InputBox('검색 저장', '이 검색 조건의 이름을 입력하세요:', '');
  if SearchName = '' then Exit;

  // 새 검색 기록 생성
  NewSearch.SearchText := edtSearch.Text;
  NewSearch.LogLevel := cbLogLevel.ItemIndex;
  NewSearch.DateFrom := DateFrom.Date;
  NewSearch.DateTo := DateTo.Date;

  // 배열에 추가
  SetLength(FSearchHistory, Length(FSearchHistory) + 1);
  FSearchHistory[High(FSearchHistory)] := NewSearch;

  // 파일에 저장
  SaveSearchHistory;

  ShowMessage('검색 조건이 저장되었습니다.');
end;

procedure TfrmLogViewer.btnLoadSearchClick(Sender: TObject);
var
  i: Integer;
  Items: TStringList;
  SelectedIdx: Integer;
begin
  if Length(FSearchHistory) = 0 then
  begin
    ShowMessage('저장된 검색 조건이 없습니다.');
    Exit;
  end;

  Items := TStringList.Create;
  try
    for i := 0 to High(FSearchHistory) do
      Items.Add(Format('검색: %s, 레벨: %d, 날짜: %s - %s',
                  [FSearchHistory[i].SearchText,
                   FSearchHistory[i].LogLevel,
                   FormatDateTime('yyyy-mm-dd', FSearchHistory[i].DateFrom),
                   FormatDateTime('yyyy-mm-dd', FSearchHistory[i].DateTo)]));

    SelectedIdx := SelectFromList('검색 불러오기', '저장된 검색 조건을 선택하세요:', Items);
    if SelectedIdx >= 0 then
    begin
      // 선택한 검색 조건 로드
      edtSearch.Text := FSearchHistory[SelectedIdx].SearchText;
      cbLogLevel.ItemIndex := FSearchHistory[SelectedIdx].LogLevel;
      DateFrom.Date := FSearchHistory[SelectedIdx].DateFrom;
      DateTo.Date := FSearchHistory[SelectedIdx].DateTo;

      // 필터 적용
      RefreshData;
    end;
  finally
    Items.Free;
  end;
end;

procedure TfrmLogViewer.chkMultiLevelClick(Sender: TObject);
begin
  // 다중 레벨 선택 모드 전환
  cbLogLevel.Visible := not chkMultiLevel.Checked;
  clbLogLevels.Visible := chkMultiLevel.Checked;

  // 모드 변경 시 필터 다시 적용
  if not FLoading then
    RefreshData;
end;

procedure TfrmLogViewer.clbLogLevelsClickCheck(Sender: TObject);
begin
  if not FClearingFilter then
    ApplyFilter;
end;

procedure TfrmLogViewer.cbLogLevelChange(Sender: TObject);
begin
  if not FClearingFilter then
    ApplyFilter;
end;

procedure TfrmLogViewer.DateFromChange(Sender: TObject);
begin
  if DateFrom.Date > DateTo.Date then
    DateTo.Date := DateFrom.Date;

  if not FClearingFilter then
    RefreshData;
end;

procedure TfrmLogViewer.DateToChange(Sender: TObject);
begin
  if DateTo.Date < DateFrom.Date then
    DateFrom.Date := DateTo.Date;

  if not FClearingFilter then
    RefreshData;
end;

procedure TfrmLogViewer.edtRefreshIntervalChange(Sender: TObject);
begin
  // 새로고침 간격 변경
  TimerRefresh.Interval := StrToIntDef(edtRefreshInterval.Text, 5) * 1000;
end;

procedure TfrmLogViewer.edtSearchChange(Sender: TObject);
begin
  if not FClearingFilter then
    RefreshData;
end;

procedure TfrmLogViewer.TimerRefreshTimer(Sender: TObject);
begin
  RefreshData;
end;

procedure TfrmLogViewer.FormClose(Sender: TObject; var CloseAction: TCloseAction);
begin
  TimerRefresh.Enabled := False;
  LogQuery.Close;
  LogConnection.Disconnect;
end;

procedure TfrmLogViewer.miExitClick(Sender: TObject);
begin
  Close;
end;

procedure TfrmLogViewer.miExportCSVClick(Sender: TObject);
begin
  SaveDialog.Title := '로그 내보내기';
  SaveDialog.DefaultExt := 'csv';
  SaveDialog.Filter := 'CSV 파일 (*.csv)|*.csv|모든 파일 (*.*)|*.*';
  SaveDialog.InitialDir := ExtractFilePath(ParamStr(0));
  SaveDialog.FileName := 'logs_' + FormatDateTime('yyyymmdd', Date) + '.csv';

  if SaveDialog.Execute then
    ExportToCSV(SaveDialog.FileName);
end;

procedure TfrmLogViewer.miExportXLSClick(Sender: TObject);
begin
  SaveDialog.Title := '로그 내보내기';
  SaveDialog.DefaultExt := 'xls';
  SaveDialog.Filter := 'XLS 파일 (*.xls)|*.xls|모든 파일 (*.*)|*.*';
  SaveDialog.InitialDir := ExtractFilePath(ParamStr(0));
  SaveDialog.FileName := 'logs_' + FormatDateTime('yyyymmdd', Date) + '.xls';

  if SaveDialog.Execute then
    ExportToXLS(SaveDialog.FileName);
end;

procedure TfrmLogViewer.miDetailLogClick(Sender: TObject);
begin
  ShowDetailLog;
end;

procedure TfrmLogViewer.miFilteredDeleteClick(Sender: TObject);
begin
  DeleteFilteredLogs;
end;

procedure TfrmLogViewer.ExportToCSV(const FileName: string);
var
  F: TextFile;
  i: Integer;
  Line, CellValue: string;
  CurrentBookmark: TBookmark;
  FileOpened: Boolean;
begin
  if LogQuery.IsEmpty then
  begin
    ShowMessage('내보낼 로그 데이터가 없습니다.');
    Exit;
  end;

  FileOpened := False;
  try
    AssignFile(F, FileName);
    Rewrite(F);
    FileOpened := True;

    // 헤더 쓰기
    Line := '';
    for i := 0 to LogQuery.FieldCount - 1 do
    begin
      if i > 0 then Line := Line + ',';
      Line := Line + '"' + LogQuery.Fields[i].DisplayLabel + '"';
    end;
    WriteLn(F, Line);

    // 현재 위치 저장
    CurrentBookmark := LogQuery.GetBookmark;

    try
      LogQuery.DisableControls;
      LogQuery.First;

      // 데이터 쓰기
      while not LogQuery.EOF do
      begin
        Line := '';
        for i := 0 to LogQuery.FieldCount - 1 do
        begin
          if i > 0 then Line := Line + ',';

          CellValue := LogQuery.Fields[i].AsString;
          // 쉼표나 큰따옴표 처리
          if (Pos(',', CellValue) > 0) or (Pos('"', CellValue) > 0) or (Pos(#13, CellValue) > 0) or (Pos(#10, CellValue) > 0) then
          begin
            CellValue := StringReplace(CellValue, '"', '""', [rfReplaceAll]);
            CellValue := '"' + CellValue + '"';
          end;

          Line := Line + CellValue;
        end;

        WriteLn(F, Line);
        LogQuery.Next;
      end;
    finally
      // 원래 위치로 복원
      if LogQuery.BookmarkValid(CurrentBookmark) then
        LogQuery.GotoBookmark(CurrentBookmark);
      LogQuery.FreeBookmark(CurrentBookmark);
      LogQuery.EnableControls;
    end;

    CloseFile(F);
    FileOpened := False;
    ShowMessage('로그가 성공적으로 내보내졌습니다: ' + FileName);
  except
    on E: Exception do
    begin
      if FileOpened then
      begin
        try
          CloseFile(F);
        except
          // 파일 닫기 중 오류 무시
        end;
      end;
      ShowMessage('로그 내보내기 중 오류가 발생했습니다: ' + E.Message);
    end;
  end;
end;

procedure TfrmLogViewer.ExportToXLS(const FileName: string);
begin
  if LogQuery.IsEmpty then
  begin
    ShowMessage('내보낼 로그 데이터가 없습니다.');
    Exit;
  end;

  RxDBGridExportSpreadSheet.RxDBGrid := RxDBGrid;
  RxDBGridExportSpreadSheet.FileName := FileName;
  RxDBGridExportSpreadSheet.OpenAfterExport := True;
  RxDBGridExportSpreadSheet.Options := [ressExportColors, ressExportFooter,
                                       ressExportFormula, ressExportTitle];
  RxDBGridExportSpreadSheet.ShowSetupForm := True;
  RxDBGridExportSpreadSheet.Execute;
end;

procedure TfrmLogViewer.UpdateRowCount;
begin
  // 로그 수 표시
  if LogQuery.IsEmpty then
    lblCount.Caption := '로그 없음'
  else
    lblCount.Caption := Format('로그 수: %d', [LogQuery.RecordCount]);
end;

procedure TfrmLogViewer.miFindDlgClick(Sender: TObject);
begin
  RxDBGrid.ShowFindDialog;
end;

procedure TfrmLogViewer.miColumnsDlgClick(Sender: TObject);
begin
  RxDBGrid.ShowColumnsDialog;
end;

procedure TfrmLogViewer.miFilterDlgClick(Sender: TObject);
begin
  RxDBGrid.ShowFilterDialog;
end;

procedure TfrmLogViewer.miSortDlgClick(Sender: TObject);
begin
  RxDBGrid.ShowSortDialog;
end;

procedure TfrmLogViewer.miOptimizeColumnsClick(Sender: TObject);
begin
  RxDBGrid.OptimizeColumnsWidthAll;
end;

procedure TfrmLogViewer.ShowDetailLog;
var
  DetailForm: TForm;
  Memo: TMemo;
  Level, Source, TimeStamp, Msg: string;
begin
  if LogQuery.IsEmpty then Exit;

  Level := LogQuery.FieldByName('LLEVEL').AsString;
  Source := LogQuery.FieldByName('LSOURCE').AsString;
  TimeStamp := FormatDateTime('yyyy-mm-dd ', LogQuery.FieldByName('LDATE').AsDateTime) +
               LogQuery.FieldByName('LTIME').AsString;
  Msg := LogQuery.FieldByName('LMESSAGE').AsString;

  DetailForm := TForm.Create(Self);
  try
    DetailForm.Caption := Format('로그 상세 정보 - %s [%s] %s', [TimeStamp, Level, Source]);
    DetailForm.Width := 600;
    DetailForm.Height := 400;
    DetailForm.Position := poScreenCenter;

    Memo := TMemo.Create(DetailForm);
    Memo.Parent := DetailForm;
    Memo.Align := alClient;
    Memo.ScrollBars := ssVertical;
    Memo.ReadOnly := True;
    Memo.Font.Name := 'Consolas';
    Memo.Font.Size := 10;

    Memo.Lines.Add('시간: ' + TimeStamp);
    Memo.Lines.Add('레벨: ' + Level);
    Memo.Lines.Add('소스: ' + Source);
    Memo.Lines.Add('');
    Memo.Lines.Add('메시지:');
    Memo.Lines.Add('--------');
    Memo.Lines.Add(Msg);

    DetailForm.ShowModal;
  finally
    DetailForm.Free;
  end;
end;

procedure TfrmLogViewer.DeleteFilteredLogs;
var
  LogLevels: TStringList;
  SearchStr: string;
  i: Integer;
  DeleteSQL: string;
  DeleteQuery: TZQuery;
begin
  if LogQuery.IsEmpty then
  begin
    ShowMessage('삭제할 로그가 없습니다.');
    Exit;
  end;

  if MessageDlg('경고', '현재 필터링된 로그 ' + IntToStr(LogQuery.RecordCount) +
                '개를 모두 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.',
     mtWarning, [mbYes, mbNo], 0) <> mrYes then
    Exit;

  try
    // 로그 레벨 목록
    LogLevels := TStringList.Create;
    try
      if chkINFO.Checked then LogLevels.Add('INFO');
      if chkDEBUG.Checked then LogLevels.Add('DEBUG');
      if chkWARN.Checked then LogLevels.Add('WARNING');
      if chkERROR.Checked then LogLevels.Add('ERROR');
      if chkFATAL.Checked then LogLevels.Add('FATAL');
      if chkDEVEL.Checked then LogLevels.Add('DEVEL');

      // 검색어
      SearchStr := Trim(edtSearch.Text);

      if Assigned(FDBHandler) then
      begin
        // 월별 테이블 방식
        // GetTablesBetweenDates 메서드가 없으므로 대체 방법 사용

        // 메타 테이블을 통해 테이블 목록 조회
        LogQuery.Close;
        LogQuery.SQL.Text :=
          'SELECT TABLE_NAME FROM ' + FDBHandler.MetaTableName + ' ' +
          'WHERE (YEAR_MONTH >= :StartYM AND YEAR_MONTH <= :EndYM)';
        LogQuery.ParamByName('StartYM').AsString := FormatDateTime('YYYYMM', DateFrom.Date);
        LogQuery.ParamByName('EndYM').AsString := FormatDateTime('YYYYMM', DateTo.Date);
        LogQuery.Open;

        // 각 테이블별로 삭제 실행
        while not LogQuery.EOF do
        begin
          DeleteSQL := 'DELETE FROM ' + LogQuery.FieldByName('TABLE_NAME').AsString +
                       ' WHERE LDATE >= :StartDate AND LDATE <= :EndDate';

          // 로그 레벨 필터 추가
          if (LogLevels.Count > 0) and (LogLevels.Count < 6) then
          begin
            DeleteSQL := DeleteSQL + ' AND (';
            for i := 0 to LogLevels.Count - 1 do
            begin
              if i > 0 then DeleteSQL := DeleteSQL + ' OR ';
              DeleteSQL := DeleteSQL + 'LLEVEL = ''' + LogLevels[i] + '''';
            end;
            DeleteSQL := DeleteSQL + ')';
          end;

          // 검색어 필터 추가
          if SearchStr <> '' then
          begin
            DeleteSQL := DeleteSQL + Format(' AND ((LSOURCE LIKE ''%%%s%%'') OR (LMESSAGE LIKE ''%%%s%%''))',
                         [SearchStr, SearchStr]);
          end;

          // 삭제 쿼리 실행
          DeleteQuery := TZQuery.Create(nil);
          try
            DeleteQuery.Connection := LogConnection;
            DeleteQuery.SQL.Text := DeleteSQL;
            DeleteQuery.ParamByName('StartDate').AsDate := DateFrom.Date;
            DeleteQuery.ParamByName('EndDate').AsDate := DateTo.Date;
            DeleteQuery.ExecSQL;
          finally
            DeleteQuery.Free;
          end;

          LogQuery.Next;
        end;
      end
      else
      begin
        // 단일 테이블 방식 (이전 버전 호환)
        DeleteSQL := 'DELETE FROM LOGS WHERE LDATE >= :StartDate AND LDATE <= :EndDate';

        // 로그 레벨 필터 추가
        if (LogLevels.Count > 0) and (LogLevels.Count < 6) then
        begin
          DeleteSQL := DeleteSQL + ' AND (';
          for i := 0 to LogLevels.Count - 1 do
          begin
            if i > 0 then DeleteSQL := DeleteSQL + ' OR ';
            DeleteSQL := DeleteSQL + 'LLEVEL = ''' + LogLevels[i] + '''';
          end;
          DeleteSQL := DeleteSQL + ')';
        end;

        // 검색어 필터 추가
        if SearchStr <> '' then
        begin
          DeleteSQL := DeleteSQL + Format(' AND ((LSOURCE LIKE ''%%%s%%'') OR (LMESSAGE LIKE ''%%%s%%''))',
                       [SearchStr, SearchStr]);
        end;

        // 삭제 쿼리 실행
        LogQuery.Close;
        LogQuery.SQL.Text := DeleteSQL;
        LogQuery.ParamByName('StartDate').AsDate := DateFrom.Date;
        LogQuery.ParamByName('EndDate').AsDate := DateTo.Date;
        LogQuery.ExecSQL;
      end;
    finally
      LogLevels.Free;
    end;

    // 데이터 새로고침
    RefreshData;
    ShowMessage('필터링된 로그가 삭제되었습니다.');
  except
    on E: Exception do
      ShowMessage('로그 삭제 중 오류가 발생했습니다: ' + E.Message);
  end;
end;

procedure TfrmLogViewer.LoadSearchHistory;
var
  IniFile: TIniFile;
  i, Count: Integer;
  Section: string;
begin
  IniFile := TIniFile.Create(ExtractFilePath(ParamStr(0)) + 'LogViewer.ini');
  try
    Count := IniFile.ReadInteger('SearchHistory', 'Count', 0);
    SetLength(FSearchHistory, Count);

    for i := 0 to Count - 1 do
    begin
      Section := 'Search_' + IntToStr(i);
      FSearchHistory[i].SearchText := IniFile.ReadString(Section, 'SearchText', '');
      FSearchHistory[i].LogLevel := IniFile.ReadInteger(Section, 'LogLevel', 0);
      FSearchHistory[i].DateFrom := StrToDateDef(IniFile.ReadString(Section, 'DateFrom', ''), Date - 7);
      FSearchHistory[i].DateTo := StrToDateDef(IniFile.ReadString(Section, 'DateTo', ''), Date);
    end;
  finally
    IniFile.Free;
  end;
end;

procedure TfrmLogViewer.SaveSearchHistory;
var
  IniFile: TIniFile;
  i: Integer;
  Section: string;
begin
  IniFile := TIniFile.Create(ExtractFilePath(ParamStr(0)) + 'LogViewer.ini');
  try
    IniFile.WriteInteger('SearchHistory', 'Count', Length(FSearchHistory));

    for i := 0 to High(FSearchHistory) do
    begin
      Section := 'Search_' + IntToStr(i);
      IniFile.WriteString(Section, 'SearchText', FSearchHistory[i].SearchText);
      IniFile.WriteInteger(Section, 'LogLevel', FSearchHistory[i].LogLevel);
      IniFile.WriteString(Section, 'DateFrom', FormatDateTime('yyyy-mm-dd', FSearchHistory[i].DateFrom));
      IniFile.WriteString(Section, 'DateTo', FormatDateTime('yyyy-mm-dd', FSearchHistory[i].DateTo));
    end;
  finally
    IniFile.Free;
  end;
end;

function SelectFromList(const Title, Prompt: string; Items: TStrings): Integer;
var
  Form: TForm;
  Label1: TLabel;
  ListBox: TListBox;
  ButtonPanel: TPanel;
  OKButton, CancelButton: TButton;
begin
  Result := -1;

  Form := TForm.Create(nil);
  try
    Form.Caption := Title;
    Form.Position := poScreenCenter;
    Form.BorderStyle := bsDialog;
    Form.Width := 350;
    Form.Height := 300;

    Label1 := TLabel.Create(Form);
    Label1.Parent := Form;
    Label1.Caption := Prompt;
    Label1.Left := 8;
    Label1.Top := 8;

    ListBox := TListBox.Create(Form);
    ListBox.Parent := Form;
    ListBox.Left := 8;
    ListBox.Top := 24;
    ListBox.Width := Form.ClientWidth - 16;
    ListBox.Height := Form.ClientHeight - 80;
    ListBox.Items.Assign(Items);
    if ListBox.Items.Count > 0 then
      ListBox.ItemIndex := 0;

    ButtonPanel := TPanel.Create(Form);
    ButtonPanel.Parent := Form;
    ButtonPanel.Align := alBottom;
    ButtonPanel.Height := 40;
    ButtonPanel.BevelOuter := bvNone;

    OKButton := TButton.Create(Form);
    OKButton.Parent := ButtonPanel;
    OKButton.Caption := '확인';
    OKButton.ModalResult := mrOK;
    OKButton.Left := ButtonPanel.Width - 160;
    OKButton.Top := 8;
    OKButton.Width := 75;

    CancelButton := TButton.Create(Form);
    CancelButton.Parent := ButtonPanel;
    CancelButton.Caption := '취소';
    CancelButton.ModalResult := mrCancel;
    CancelButton.Left := ButtonPanel.Width - 80;
    CancelButton.Top := 8;
    CancelButton.Width := 75;

    if Form.ShowModal = mrOK then
      Result := ListBox.ItemIndex;
  finally
    Form.Free;
  end;
end;

end.
