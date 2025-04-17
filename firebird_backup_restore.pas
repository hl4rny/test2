// Firebird, Zeoslib의 TZConnection을 사용하여 백업/복구 수행
uses
  ZConnection, ZScriptParser;

// 백업 함수
procedure BackupWithZeos(Connection: TZConnection; const BackupFile: string);
var
  Script: TZSQLProcessor;
begin
  Script := TZSQLProcessor.Create(nil);
  try
    Script.Connection := Connection;
    Script.Script.Text := 'backup database ''' + Connection.Database + 
                          ''' to ''' + BackupFile + '''';
    Script.Execute;
  finally
    Script.Free;
  end;
end;

// 복구 함수
procedure RestoreWithZeos(Connection: TZConnection; const BackupFile: string);
var
  Script: TZSQLProcessor;
begin
  Script := TZSQLProcessor.Create(nil);
  try
    Script.Connection := Connection;
    Script.Script.Text := 'restore database ''' + BackupFile + 
                          ''' to ''' + Connection.Database + '''';
    Script.Execute;
  finally
    Script.Free;
  end;
end;


// 파일 복사 방식 (Embedded 전용)
// Firebird Embedded를 사용하는 경우 데이터베이스가 단일 파일이므로 간단히 파일을 복사할 수 있습니다.
{
주의사항
  백업/복구 작업 시 데이터베이스 연결을 완전히 끊어야 합니다.
  Embedded 버전에서는 동시 접근이 불가능하므로 애플리케이션을 완전히 종료한 후 작업해야 합니다.
  대용량 데이터베이스의 경우 백업/복구 시간이 오래 걸릴 수 있습니다.
  백업 파일 확장자로 .fbk를 사용하는 것이 일반적입니다.	
}
uses
  FileUtil;

// 파일 복사 백업
procedure CopyBackup(const SourceDB, BackupFile: string);
begin
  if FileExists(SourceDB) then
    CopyFile(SourceDB, BackupFile);
end;

// 주의: 복구 시에는 데이터베이스 연결이 완전히 끊어진 상태여야 함
procedure CopyRestore(const BackupFile, TargetDB: string);
begin
  if FileExists(BackupFile) then
  begin
    // 기존 파일 삭제
    if FileExists(TargetDB) then
      DeleteFile(TargetDB);
    CopyFile(BackupFile, TargetDB);
  end;
end;




