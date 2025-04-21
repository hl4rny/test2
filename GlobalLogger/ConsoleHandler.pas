unit ConsoleHandler;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, LogHandlers;

type
  { TConsoleHandler - 콘솔 로그 핸들러 }
  TConsoleHandler = class(TLogHandler)
  private
    FUseColors: Boolean;        // 색상 사용 여부
    FShowLevelPrefix: Boolean;  // 레벨 접두사 표시 여부

  protected
    procedure WriteLog(const Msg: string; Level: TLogLevel); override;

    // 로그 레벨에 따른 색상 가져오기
    function GetColorForLevel(Level: TLogLevel): Integer;

    // 로그 레벨 접두사 가져오기
    function GetPrefixForLevel(Level: TLogLevel): string;

  public
    constructor Create; override;

    // 속성
    property UseColors: Boolean read FUseColors write FUseColors;
    property ShowLevelPrefix: Boolean read FShowLevelPrefix write FShowLevelPrefix;
  end;

implementation

{$IFDEF WINDOWS}
uses Windows;

// Windows용 콘솔 정보 구조체 (외부 선언 없을 경우 대비)
type
  TConsoleScreenBufferInfo = record
    dwSize: TCoord;
    dwCursorPosition: TCoord;
    wAttributes: Word;
    srWindow: TSmallRect;
    dwMaximumWindowSize: TCoord;
  end;
{$ENDIF}

{ TConsoleHandler }

constructor TConsoleHandler.Create;
begin
  inherited;

  FUseColors := True;
  FShowLevelPrefix := True;
end;

procedure TConsoleHandler.WriteLog(const Msg: string; Level: TLogLevel);
var
  LevelPrefix, OutputMsg: string;
  {$IFDEF WINDOWS}
  OldColor, NewColor: Word;
  StdOut: THandle;
  //ConsoleInfo: TConsoleScreenBufferInfo;
  ConsoleInfo: CONSOLE_SCREEN_BUFFER_INFO;  // 타입 변경
  {$ENDIF}
begin
  if FShowLevelPrefix then
    LevelPrefix := GetPrefixForLevel(Level)
  else
    LevelPrefix := '';

  // 출력할 메시지 준비
  if LevelPrefix <> '' then
    OutputMsg := LevelPrefix + ' ' + Msg
  else
    OutputMsg := Msg;

  {$IFDEF WINDOWS}
  if FUseColors then
  begin
    try
      // Windows에서는 콘솔 색상 변경 가능
      StdOut := GetStdHandle(STD_OUTPUT_HANDLE);

      // 현재 색상 저장
      if GetConsoleScreenBufferInfo(StdOut, ConsoleInfo) then
      begin
        OldColor := ConsoleInfo.wAttributes;

        // 레벨에 따른 새 색상 설정
        NewColor := GetColorForLevel(Level);
        SetConsoleTextAttribute(StdOut, NewColor);

        // 메시지 출력
        WriteLn(OutputMsg);

        // 원래 색상으로 복원
        SetConsoleTextAttribute(StdOut, OldColor);
      end
      else
      begin
        // 콘솔 정보를 얻을 수 없는 경우 일반 출력
        WriteLn(OutputMsg);
      end;
    except
      // 색상 변경 실패 시 일반 출력
      WriteLn(OutputMsg);
    end;
  end
  else
  {$ENDIF}
  begin
    // 일반 출력
    WriteLn(OutputMsg);
  end;
end;

function TConsoleHandler.GetColorForLevel(Level: TLogLevel): Integer;
begin
  {$IFDEF WINDOWS}
  case Level of
    llDevelop:  Result := FOREGROUND_GREEN;  // 녹색
    llDebug:    Result := FOREGROUND_GREEN or FOREGROUND_BLUE;  // 청록색
    llInfo:     Result := FOREGROUND_RED or FOREGROUND_GREEN or FOREGROUND_BLUE;  // 흰색
    llWarning:  Result := FOREGROUND_RED or FOREGROUND_GREEN or FOREGROUND_INTENSITY;  // 밝은 노란색
    llError:    Result := FOREGROUND_RED or FOREGROUND_INTENSITY;  // 밝은 빨간색
    llFatal:    Result := FOREGROUND_RED or FOREGROUND_BLUE or FOREGROUND_INTENSITY;  // 밝은 자주색
    else        Result := FOREGROUND_RED or FOREGROUND_GREEN or FOREGROUND_BLUE;  // 기본 흰색
  end;
  {$ELSE}
  Result := 0; // 다른 플랫폼에서는 사용하지 않음
  {$ENDIF}
end;

function TConsoleHandler.GetPrefixForLevel(Level: TLogLevel): string;
begin
  case Level of
    llDevelop:  Result := '[DEVELOP]';
    llDebug:    Result := '[DEBUG]';
    llInfo:     Result := '[INFO]';
    llWarning:  Result := '[WARNING]';
    llError:    Result := '[ERROR]';
    llFatal:    Result := '[FATAL]';
    else        Result := '';
  end;
end;

end.
