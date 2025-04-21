unit SourceIdentifier;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, LogHandlers, SyncObjs;

type
  TSourceInfo = record
    HostName: string;
    IPAddress: string;
    ApplicationName: string;
    ApplicationVersion: string;
    UserName: string;
    ProcessID: Cardinal;
    UniqueInstanceID: string;
  end;

  TSourceIdentifierFormat = (
    sfHostName,
    sfIPAddress,
    sfApplication,
    sfHostAndApp,
    sfIPAndApp,
    sfFull,
    sfCustom
  );

  TSourceIdentifierHandler = class(TLogHandler)
  private
    FWrappedHandler: TLogHandler;
    FOwnsHandler: Boolean;
    FSourceInfo: TSourceInfo;
    FFormat: TSourceIdentifierFormat;
    FCustomFormat: string;

    procedure CollectSourceInfo;
    function GetIdentifierString: string;
    function FormatCustomString(const FormatStr: string): string;

  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;

  public
    constructor Create(AHandler: TLogHandler; OwnsHandler: Boolean = True); reintroduce;
    destructor Destroy; override;

    procedure Init; override;
    procedure Shutdown; override;
    procedure DeliverWithTag(const Msg: string; Level: TLogLevel; const Tag: string);

    property WrappedHandler: TLogHandler read FWrappedHandler;
    property Format: TSourceIdentifierFormat read FFormat write FFormat;
    property CustomFormat: string read FCustomFormat write FCustomFormat;
    property SourceInfo: TSourceInfo read FSourceInfo write FSourceInfo;
  end;

implementation

uses
  blcksock
  {$IFDEF MSWINDOWS}
  , Windows
  {$ENDIF}
  {$IFDEF UNIX}
  , BaseUnix
  {$ENDIF}
  ;

{ TSourceIdentifierHandler }

constructor TSourceIdentifierHandler.Create(AHandler: TLogHandler; OwnsHandler: Boolean);
begin
  inherited Create;
  FWrappedHandler := AHandler;
  FOwnsHandler := OwnsHandler;
  FFormat := sfHostAndApp;
  FCustomFormat := '[%HOST%][%APP%]';
  FillChar(FSourceInfo, SizeOf(FSourceInfo), 0);
  CollectSourceInfo;
end;

destructor TSourceIdentifierHandler.Destroy;
begin
  if FOwnsHandler and Assigned(FWrappedHandler) then
    FWrappedHandler.Free;
  inherited;
end;

procedure TSourceIdentifierHandler.Init;
begin
  inherited;
  if Assigned(FWrappedHandler) then
    FWrappedHandler.Init;
end;

procedure TSourceIdentifierHandler.Shutdown;
begin
  if Assigned(FWrappedHandler) then
    FWrappedHandler.Shutdown;
  inherited;
end;

procedure TSourceIdentifierHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  IdentifiedMsg, IdString: string;
begin
  if not Assigned(FWrappedHandler) then Exit;
  IdString := GetIdentifierString;

  if IdString <> '' then
    IdentifiedMsg := IdString + ' ' + Msg
  else
    IdentifiedMsg := Msg;

  FWrappedHandler.Deliver(IdentifiedMsg, Level, '');
end;

procedure TSourceIdentifierHandler.DeliverWithTag(const Msg: string; Level: TLogLevel; const Tag: string);
var
  IdentifiedMsg, IdString: string;
begin
  if not Assigned(FWrappedHandler) then Exit;
  IdString := GetIdentifierString;

  if IdString <> '' then
    IdentifiedMsg := IdString + ' ' + Msg
  else
    IdentifiedMsg := Msg;

  FWrappedHandler.Deliver(IdentifiedMsg, Level, Tag);
end;

procedure TSourceIdentifierHandler.CollectSourceInfo;
var
  Socket: TTCPBlockSocket;
  Buffer: array[0..255] of Char;
  Len: DWORD;
begin
  Socket := TTCPBlockSocket.Create;
  try
    try
      // 호스트 이름 가져오기
      FSourceInfo.HostName := Socket.LocalName;
      if FSourceInfo.HostName = '' then
        FSourceInfo.HostName := 'unknown-host';

      // IP 주소 가져오기
      FSourceInfo.IPAddress := Socket.ResolveName(FSourceInfo.HostName);
      if FSourceInfo.IPAddress = '' then
        FSourceInfo.IPAddress := '127.0.0.1';
    except
      FSourceInfo.HostName := 'unknown-host';
      FSourceInfo.IPAddress := '127.0.0.1';
    end;
  finally
    Socket.Free;
  end;

  FSourceInfo.ApplicationName := ExtractFileName(ParamStr(0));
  FSourceInfo.ApplicationVersion := '1.0.0';

  {$IFDEF MSWINDOWS}
  FillChar(Buffer, SizeOf(Buffer), 0);
  Len := GetEnvironmentVariable('USERNAME', Buffer, SizeOf(Buffer));
  if Len > 0 then
    FSourceInfo.UserName := Buffer
  else
    FSourceInfo.UserName := 'unknown-user';
  FSourceInfo.ProcessID := GetCurrentProcessId;
  {$ELSE}
  FSourceInfo.UserName := GetEnvironmentVariable('USER');
  if FSourceInfo.UserName = '' then
    FSourceInfo.UserName := 'unknown-user';
  FSourceInfo.ProcessID := fpGetPID;
  {$ENDIF}

  Randomize;
  FSourceInfo.UniqueInstanceID := SysUtils.Format('%s-%d-%d',
    [FormatDateTime('yyyymmddhhnnsszzz', Now),
     FSourceInfo.ProcessID,
     Random(1000000)]);
end;

function TSourceIdentifierHandler.GetIdentifierString: string;
begin
  case FFormat of
    sfHostName:
      Result := SysUtils.Format('[%s]', [FSourceInfo.HostName]);
    sfIPAddress:
      Result := SysUtils.Format('[%s]', [FSourceInfo.IPAddress]);
    sfApplication:
      Result := SysUtils.Format('[%s %s]', [FSourceInfo.ApplicationName, FSourceInfo.ApplicationVersion]);
    sfHostAndApp:
      Result := SysUtils.Format('[%s][%s]', [FSourceInfo.HostName, FSourceInfo.ApplicationName]);
    sfIPAndApp:
      Result := SysUtils.Format('[%s][%s]', [FSourceInfo.IPAddress, FSourceInfo.ApplicationName]);
    sfFull:
      Result := SysUtils.Format('[%s][%s][%s %s][%s][%d]',
        [FSourceInfo.HostName,
         FSourceInfo.IPAddress,
         FSourceInfo.ApplicationName,
         FSourceInfo.ApplicationVersion,
         FSourceInfo.UserName,
         FSourceInfo.ProcessID]);
    sfCustom:
      Result := FormatCustomString(FCustomFormat);
  end;
end;

function TSourceIdentifierHandler.FormatCustomString(const FormatStr: string): string;
var
  TempStr: string;
begin
  TempStr := FormatStr;
  TempStr := StringReplace(TempStr, '%HOST%', FSourceInfo.HostName, [rfReplaceAll, rfIgnoreCase]);
  TempStr := StringReplace(TempStr, '%IP%', FSourceInfo.IPAddress, [rfReplaceAll, rfIgnoreCase]);
  TempStr := StringReplace(TempStr, '%APP%', FSourceInfo.ApplicationName, [rfReplaceAll, rfIgnoreCase]);
  TempStr := StringReplace(TempStr, '%VER%', FSourceInfo.ApplicationVersion, [rfReplaceAll, rfIgnoreCase]);
  TempStr := StringReplace(TempStr, '%USER%', FSourceInfo.UserName, [rfReplaceAll, rfIgnoreCase]);
  TempStr := StringReplace(TempStr, '%PID%', IntToStr(FSourceInfo.ProcessID), [rfReplaceAll, rfIgnoreCase]);
  TempStr := StringReplace(TempStr, '%ID%', FSourceInfo.UniqueInstanceID, [rfReplaceAll, rfIgnoreCase]);
  Result := TempStr;
end;

end.
