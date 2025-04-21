unit LogEmailHandler;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, LogHandlers, SyncObjs, DateUtils, 
  // 이메일 전송을 위한 synapse 컴포넌트 사용
  smtpsend, mimemess, mimepart, synachar, synautil;

type
  // 이메일 형식 타입
  TEmailFormat = (efPlainText, efHTML);
  
  // 이메일 전송 스레드 전방 선언
  TEmailSenderThread = class;
  
  // 이메일 대기열 항목
  TEmailQueueItem = record
    Subject: string;
    Message: string;
    Level: TLogLevel;
    Timestamp: TDateTime;
  end;
  PEmailQueueItem = ^TEmailQueueItem;
  
{ TLogEmailHandler - 이메일 알림 핸들러 }
  TLogEmailHandler = class(TLogHandler)
  private
    FLock: TCriticalSection;  // 스레드 동기화 객체

    // SMTP 설정
    FSmtpServer: string;
    FSmtpPort: Integer;
    FSmtpUser: string;
    FSmtpPassword: string;
    FUseSSL: Boolean;
    
    // 이메일 설정
    FFromAddress: string;
    FToAddresses: TStringList;
    FCcAddresses: TStringList;
    FEmailSubjectPrefix: string;
    FEmailFormat: TEmailFormat;
    
    // 전송 제어
    FMaxEmailsPerHour: Integer;    // 시간당 최대 이메일 수
    FMinTimeBetweenEmails: Integer; // 이메일 사이 최소 시간(초)
    FBatchEmails: Boolean;         // 여러 로그를 한 이메일로 묶기
    FBatchInterval: Integer;       // 배치 처리 간격(초)
    FSentEmailsCount: Integer;     // 시간 내 전송된 이메일 수
    FSentEmailsResetTime: TDateTime; // 카운터 초기화 시간
    FLastEmailTime: TDateTime;     // 마지막 이메일 전송 시간
    
    // 이메일 로그 레벨 필터
    FEmailLogLevels: TLogLevelSet;
    
    // 이메일 큐 및 처리 스레드
    FEmailQueue: TThreadList;
    FEmailThread: TEmailSenderThread;
    FEmailThreadRunning: Boolean;
    
    // 메서드
    function FormatEmailBody(const LogItems: TList; EmailFormat: TEmailFormat): string;
    function GetFormattedSubject(Level: TLogLevel): string;
    function SendEmailDirect(const Subject, Body: string): Boolean;
    function CanSendEmail: Boolean;
    procedure UpdateEmailStats;
    procedure StartEmailThread;
    procedure StopEmailThread;
    function GetToAddressesAsString: string;
    procedure SetToAddressesAsString(const Addresses: string);
    function GetCcAddressesAsString: string;
    procedure SetCcAddressesAsString(const Addresses: string);
    
  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;
    
  public
    constructor Create; override;
    destructor Destroy; override;
    
    procedure Init; override;
    procedure Shutdown; override;
    
    // 수동 이메일 전송
    function SendEmail(const Subject, Body: string): Boolean;
    
    // 메일 대기열 플러시
    procedure FlushEmailQueue;
    
    // 이메일 주소 관리
    procedure AddToAddress(const EmailAddress: string);
    procedure AddCcAddress(const EmailAddress: string);
    procedure ClearToAddresses;
    procedure ClearCcAddresses;
    
    // 속성
    property SmtpServer: string read FSmtpServer write FSmtpServer;
    property SmtpPort: Integer read FSmtpPort write FSmtpPort;
    property SmtpUser: string read FSmtpUser write FSmtpUser;
    property SmtpPassword: string read FSmtpPassword write FSmtpPassword;
    property UseSSL: Boolean read FUseSSL write FUseSSL;
    property FromAddress: string read FFromAddress write FFromAddress;
    property ToAddressesAsString: string read GetToAddressesAsString write SetToAddressesAsString;
    property CcAddressesAsString: string read GetCcAddressesAsString write SetCcAddressesAsString;
    property EmailSubjectPrefix: string read FEmailSubjectPrefix write FEmailSubjectPrefix;
    property EmailFormat: TEmailFormat read FEmailFormat write FEmailFormat;
    property MaxEmailsPerHour: Integer read FMaxEmailsPerHour write FMaxEmailsPerHour;
    property MinTimeBetweenEmails: Integer read FMinTimeBetweenEmails write FMinTimeBetweenEmails;
    property BatchEmails: Boolean read FBatchEmails write FBatchEmails;
    property BatchInterval: Integer read FBatchInterval write FBatchInterval;
    property EmailLogLevels: TLogLevelSet read FEmailLogLevels write FEmailLogLevels;
  end;
  
  { TEmailSenderThread - 이메일 전송 스레드 }
  TEmailSenderThread = class(TThread)
  private
    FOwner: TLogEmailHandler;
    FEvent: TEvent;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TLogEmailHandler);
    destructor Destroy; override;
    procedure SignalWorkAvailable;
  end;

implementation

{ TEmailSenderThread }

constructor TEmailSenderThread.Create(AOwner: TLogEmailHandler);
begin
  inherited Create(True);  // 일시 중단 상태로 생성
  
  FOwner := AOwner;
  FEvent := TEvent.Create(nil, False, False, '');
  
  FreeOnTerminate := False;
end;

destructor TEmailSenderThread.Destroy;
begin
  FEvent.Free;
  
  inherited;
end;

procedure TEmailSenderThread.Execute;
var
  WaitResult: TWaitResult;
begin
  while not Terminated do
  begin
    // 작업 대기 (최대 지정된 배치 간격 또는 이벤트 신호)
    WaitResult := FEvent.WaitFor(FOwner.BatchInterval * 1000);
    
    if Terminated then
      Break;
      
    // 이메일 큐 처리
    FOwner.FlushEmailQueue;
  end;
end;

procedure TEmailSenderThread.SignalWorkAvailable;
begin
  FEvent.SetEvent;
end;

{ TLogEmailHandler }

constructor TLogEmailHandler.Create;
begin
  inherited;
  
  // SMTP 기본 설정
  FSmtpServer := 'localhost';
  FSmtpPort := 25;
  FSmtpUser := '';
  FSmtpPassword := '';
  FUseSSL := False;
  
  // 이메일 기본 설정
  FFromAddress := 'logger@application.com';
  FToAddresses := TStringList.Create;
  FCcAddresses := TStringList.Create;
  FEmailSubjectPrefix := '[로그 알림]';
  FEmailFormat := efHTML;
  
  // 전송 제어 기본값
  FMaxEmailsPerHour := 10;
  FMinTimeBetweenEmails := 60;  // 1분
  FBatchEmails := True;
  FBatchInterval := 300;        // 5분
  FSentEmailsCount := 0;
  FSentEmailsResetTime := Now;
  FLastEmailTime := 0;
  
  // 기본적으로 Error 및 Fatal 로그만 이메일 전송
  FEmailLogLevels := [llError, llFatal];
  
  // 이메일 큐 초기화
  FEmailQueue := TThreadList.Create;
  FEmailThread := nil;
  FEmailThreadRunning := False;
end;

destructor TLogEmailHandler.Destroy;
begin
  Shutdown;
  
  // 리소스 정리
  FToAddresses.Free;
  FCcAddresses.Free;
  
  // 이메일 큐 정리
  FlushEmailQueue;
  FEmailQueue.Free;
  
  inherited;
end;

procedure TLogEmailHandler.Init;
begin
  inherited;
  
  // 이메일 스레드 시작
  if FBatchEmails then
    StartEmailThread;
end;

procedure TLogEmailHandler.Shutdown;
begin
  // 이메일 스레드 정지
  StopEmailThread;
  
  // 남은 메일 대기열 처리
  FlushEmailQueue;
  
  inherited;
end;

procedure TLogEmailHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  QueueItem: PEmailQueueItem;
begin
  // 레벨 필터 확인
  if not (Level in FEmailLogLevels) then
    Exit;
  
  // 이메일 큐 항목 생성
  New(QueueItem);
  QueueItem^.Subject := GetFormattedSubject(Level);
  QueueItem^.Message := Msg;
  QueueItem^.Level := Level;
  QueueItem^.Timestamp := Now;
  
  // 이메일 큐에 추가
  FEmailQueue.Add(QueueItem);
  
  // 배치 모드가 아니면 즉시 전송 시도
  if not FBatchEmails then
    FlushEmailQueue
  else if Assigned(FEmailThread) then
    TEmailSenderThread(FEmailThread).SignalWorkAvailable;
end;

function TLogEmailHandler.SendEmail(const Subject, Body: string): Boolean;
begin
  if not CanSendEmail then
  begin
    Result := False;
    Exit;
  end;
  
  Result := SendEmailDirect(Subject, Body);
  
  if Result then
    UpdateEmailStats;
end;

procedure TLogEmailHandler.FlushEmailQueue;
var
  List: TList;
  i: Integer;
  QueueItem: PEmailQueueItem;
  EmailBody: string;
begin
  if not CanSendEmail then
    Exit;
  
  List := FEmailQueue.LockList;
  try
    if List.Count = 0 then
      Exit;
      
    // 이메일 본문 생성
    EmailBody := FormatEmailBody(List, FEmailFormat);
    
    // 첫 번째 항목의 제목 사용
    QueueItem := PEmailQueueItem(List[0]);
    
    // 이메일 전송
    if SendEmailDirect(QueueItem^.Subject, EmailBody) then
    begin
      // 전송 성공 시 통계 업데이트
      UpdateEmailStats;
      
      // 큐 정리
      for i := 0 to List.Count - 1 do
      begin
        QueueItem := PEmailQueueItem(List[i]);
        Dispose(QueueItem);
      end;
      
      // 처리 완료된 항목 제거
      List.Clear;
    end;
  finally
    FEmailQueue.UnlockList;
  end;
end;

function TLogEmailHandler.FormatEmailBody(const LogItems: TList; EmailFormat: TEmailFormat): string;
var
  i: Integer;
  QueueItem: PEmailQueueItem;
  LevelStr: string;
  TimeStr: string;
  MsgText: string;
begin
  Result := '';

  if EmailFormat = efHTML then
  begin
    // HTML 형식
    Result := '<!DOCTYPE html>' + #13#10 +
              '<html>' + #13#10 +
              '<head>' + #13#10 +
              '  <style>' + #13#10 +
              '    body { font-family: Arial, sans-serif; }' + #13#10 +
              '    table { border-collapse: collapse; width: 100%; }' + #13#10 +
              '    th, td { border: 1px solid #ddd; padding: 8px; }' + #13#10 +
              '    th { background-color: #f2f2f2; text-align: left; }' + #13#10 +
              '    .error { color: red; }' + #13#10 +
              '    .warning { color: orange; }' + #13#10 +
              '    .fatal { color: darkred; font-weight: bold; }' + #13#10 +
              '  </style>' + #13#10 +
              '</head>' + #13#10 +
              '<body>' + #13#10 +
              '  <h2>로그 알림</h2>' + #13#10 +
              '  <p>다음 로그 항목이 기록되었습니다:</p>' + #13#10 +
              '  <table>' + #13#10 +
              '    <tr><th>시간</th><th>레벨</th><th>메시지</th></tr>' + #13#10;

    for i := 0 to LogItems.Count - 1 do
    begin
      QueueItem := PEmailQueueItem(LogItems[i]);

      // 로그 레벨 문자열 변환
      case QueueItem^.Level of
        llDevelop:  LevelStr := 'DEVELOP';
        llDebug:    LevelStr := 'DEBUG';
        llInfo:     LevelStr := 'INFO';
        llWarning:  LevelStr := '<span class="warning">WARNING</span>';
        llError:    LevelStr := '<span class="error">ERROR</span>';
        llFatal:    LevelStr := '<span class="fatal">FATAL</span>';
      end;

      // 시간 문자열 형식화
      TimeStr := FormatDateTime('yyyy-mm-dd hh:nn:ss', QueueItem^.Timestamp);

      // HTML 이스케이프
      MsgText := StringReplace(QueueItem^.Message, '<', '&lt;', [rfReplaceAll]);
      MsgText := StringReplace(MsgText, '>', '&gt;', [rfReplaceAll]);

      // 테이블 행 추가
      Result := Result + Format('    <tr><td>%s</td><td>%s</td><td>%s</td></tr>%s',
                              [TimeStr, LevelStr, MsgText, #13#10]);
    end;

    Result := Result +
              '  </table>' + #13#10 +
              '  <p>이 이메일은 자동으로 생성되었습니다.</p>' + #13#10 +
              '</body>' + #13#10 +
              '</html>';
  end
  else
  begin
    // 일반 텍스트 형식
    Result := '=== 로그 알림 ===' + #13#10 + #13#10 +
              '다음 로그 항목이 기록되었습니다:' + #13#10 + #13#10;

    for i := 0 to LogItems.Count - 1 do
    begin
      QueueItem := PEmailQueueItem(LogItems[i]);

      // 로그 레벨 문자열 변환
      case QueueItem^.Level of
        llDevelop:  LevelStr := 'DEVELOP';
        llDebug:    LevelStr := 'DEBUG';
        llInfo:     LevelStr := 'INFO';
        llWarning:  LevelStr := 'WARNING';
        llError:    LevelStr := 'ERROR';
        llFatal:    LevelStr := 'FATAL';
      end;

      // 시간 문자열 형식화
      TimeStr := FormatDateTime('yyyy-mm-dd hh:nn:ss', QueueItem^.Timestamp);

      // 로그 항목 추가
      Result := Result + Format('[%s] [%s] %s%s',
                              [TimeStr, LevelStr, QueueItem^.Message, #13#10]);
    end;

    Result := Result + #13#10 + '이 이메일은 자동으로 생성되었습니다.';
  end;
end;

function TLogEmailHandler.GetFormattedSubject(Level: TLogLevel): string;
var
  LevelStr: string;
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
  
  // 제목 형식화
  if FEmailSubjectPrefix <> '' then
    Result := FEmailSubjectPrefix + ' ' + LevelStr
  else
    Result := '로그 알림: ' + LevelStr;
end;


function TLogEmailHandler.SendEmailDirect(const Subject, Body: string): Boolean;
var
  Smtp: TSMTPSend;
  Mime: TMimeMess;
  Part: TMimePart;
  BodyList: TStringList;
  i: Integer;
  ContentType: string;
begin
  Result := False;

  if FToAddresses.Count = 0 then
    Exit;

  Smtp := TSMTPSend.Create;
  Mime := TMimeMess.Create;
  BodyList := TStringList.Create;

  try
    Smtp.TargetHost := FSmtpServer;
    Smtp.TargetPort := IntToStr(FSmtpPort);
    Smtp.Username := FSmtpUser;
    Smtp.Password := FSmtpPassword;
    if FUseSSL then
      Smtp.FullSSL := True;

    Mime.Header.CharsetCode := UTF_8; // 문자열 아님
    Mime.Header.From := FFromAddress;
    Mime.Header.Subject := Subject;

    for i := 0 to FToAddresses.Count - 1 do
      Mime.Header.ToList.Add(FToAddresses[i]);

    for i := 0 to FCcAddresses.Count - 1 do
      Mime.Header.CCList.Add(FCcAddresses[i]);

    if FEmailFormat = efHTML then
      ContentType := 'text/html; charset=UTF-8'
    else
      ContentType := 'text/plain; charset=UTF-8';

    BodyList.Text := Body;
    Part := Mime.AddPartText(BodyList, nil);
    Part.Primary := 'text';
    if FEmailFormat = efHTML then
      Part.Secondary := 'html'
    else
      Part.Secondary := 'plain';

    Mime.EncodeMessage;

    if Smtp.Login then
    begin
      if Smtp.MailFrom(FFromAddress, Length(FFromAddress)) then
      begin
        for i := 0 to FToAddresses.Count - 1 do
            Smtp.MailTo(FToAddresses[i]);

        for i := 0 to FCcAddresses.Count - 1 do
            Smtp.MailTo(FCcAddresses[i]);

        Result := Smtp.MailData(Mime.Lines);
      end;
      Smtp.Logout;
    end;
  finally
    BodyList.Free;
    Mime.Free;
    Smtp.Free;
  end;
end;

function TLogEmailHandler.CanSendEmail: Boolean;
var
  CurrentTime: TDateTime;
  ElapsedHours, ElapsedSeconds: Int64;
begin
  Result := False;
  
  // 수신자 확인
  if FToAddresses.Count = 0 then
    Exit;
    
  // SMTP 설정 확인
  if (FSmtpServer = '') then
    Exit;
    
  // 현재 시간
  CurrentTime := Now;
  
  // 시간당 이메일 수 제한 확인
  ElapsedHours := HoursBetween(CurrentTime, FSentEmailsResetTime);
  
  if ElapsedHours >= 1 then
  begin
    // 1시간 경과, 카운터 초기화
    FSentEmailsCount := 0;
    FSentEmailsResetTime := CurrentTime;
  end
  else if FSentEmailsCount >= FMaxEmailsPerHour then
  begin
    // 시간당 이메일 수 제한 초과
    Exit;
  end;
  
  // 이메일 사이 최소 시간 확인
  if FLastEmailTime > 0 then
  begin
    ElapsedSeconds := SecondsBetween(CurrentTime, FLastEmailTime);
    
    if ElapsedSeconds < FMinTimeBetweenEmails then
      Exit;
  end;
  
  Result := True;
end;

procedure TLogEmailHandler.UpdateEmailStats;
begin
  // 마지막 이메일 전송 시간 및 카운터 업데이트
  FLastEmailTime := Now;
  Inc(FSentEmailsCount);
end;

procedure TLogEmailHandler.StartEmailThread;
begin
  if FEmailThreadRunning then
    Exit;
    
  // 이메일 전송 스레드 생성 및 시작
  FEmailThread := TEmailSenderThread.Create(Self);
  TEmailSenderThread(FEmailThread).Start;
  FEmailThreadRunning := True;
end;

procedure TLogEmailHandler.StopEmailThread;
begin
  if not FEmailThreadRunning then
    Exit;
    
  // 이메일 전송 스레드 정지
  if Assigned(FEmailThread) then
  begin
    TEmailSenderThread(FEmailThread).Terminate;
    TEmailSenderThread(FEmailThread).WaitFor;
    FEmailThread.Free;
    FEmailThread := nil;
  end;
  
  FEmailThreadRunning := False;
end;

procedure TLogEmailHandler.AddToAddress(const EmailAddress: string);
begin
  if EmailAddress = '' then
    Exit;
    
  FLock.Enter;
  try
    if FToAddresses.IndexOf(EmailAddress) < 0 then
      FToAddresses.Add(EmailAddress);
  finally
    FLock.Leave;
  end;
end;

procedure TLogEmailHandler.AddCcAddress(const EmailAddress: string);
begin
  if EmailAddress = '' then
    Exit;
    
  FLock.Enter;
  try
    if FCcAddresses.IndexOf(EmailAddress) < 0 then
      FCcAddresses.Add(EmailAddress);
  finally
    FLock.Leave;
  end;
end;

procedure TLogEmailHandler.ClearToAddresses;
begin
  FLock.Enter;
  try
    FToAddresses.Clear;
  finally
    FLock.Leave;
  end;
end;

procedure TLogEmailHandler.ClearCcAddresses;
begin
  FLock.Enter;
  try
    FCcAddresses.Clear;
  finally
    FLock.Leave;
  end;
end;

function TLogEmailHandler.GetToAddressesAsString: string;
begin
  FLock.Enter;
  try
    Result := FToAddresses.CommaText;
  finally
    FLock.Leave;
  end;
end;

procedure TLogEmailHandler.SetToAddressesAsString(const Addresses: string);
begin
  FLock.Enter;
  try
    FToAddresses.CommaText := Addresses;
  finally
    FLock.Leave;
  end;
end;

function TLogEmailHandler.GetCcAddressesAsString: string;
begin
  FLock.Enter;
  try
    Result := FCcAddresses.CommaText;
  finally
    FLock.Leave;
  end;
end;

procedure TLogEmailHandler.SetCcAddressesAsString(const Addresses: string);
begin
  FLock.Enter;
  try
    FCcAddresses.CommaText := Addresses;
  finally
    FLock.Leave;
  end;
end;

end.
