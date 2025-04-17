{
JSON 파일을 리소스로 등록하는 방법
프로젝트 디렉토리에 JSON 파일 추가 (예: data.json)
리소스 이름은 대소문자를 구분합니다.
}

.lpr 파일에 리소스 등록
{$R *.res}
{$R 'data.json' 'data.json'}


또는 .lpi 파일에 리소스 섹션 추가
<Resource>
  <Filename Value="data.json"/>
  <Name Value="DATA_JSON"/>
</Resource>


{ 리소스로 등록된 JSON 사용 방법
  리소스 이름 확인
  .lpi 파일에 <Name Value="DATA_JSON"/>으로 지정한 이름과 일치해야 합니다.
  대소문자를 정확히 맞춰야 합니다. 
}

// 리소스 스트림으로 읽기
uses
  Classes, SysUtils, fpjson, jsonparser;

procedure LoadJSONFromResource;
var
  ResStream: TResourceStream;
  JSONData: TJSONData;
begin
  ResStream := TResourceStream.Create(HInstance, 'DATA_JSON', RT_RCDATA);
  try
    JSONData := GetJSON(ResStream);
    try
      // JSON 데이터 사용
      ShowMessage(JSONData.FormatJSON);
    finally
      JSONData.Free;
    end;
  finally
    ResStream.Free;
  end;
end;

// 문자열로 변환하여 사용
var
  ResStream: TResourceStream;
  JSONString: string;
begin
  ResStream := TResourceStream.Create(HInstance, 'DATA_JSON', RT_RCDATA);
  try
    SetLength(JSONString, ResStream.Size);
    ResStream.ReadBuffer(Pointer(JSONString)^, Length(JSONString));
    // JSONString 사용
  finally
    ResStream.Free;
  end;
end;



// 이미 파싱 함수가 있는 경우
// 리소스에서 JSON 데이터 불러오기
uses
  Classes, SysUtils, LResources;

function LoadJSONFromResource(const ResourceName: string): string;
var
  ResStream: TResourceStream;
begin
  ResStream := TResourceStream.Create(HInstance, ResourceName, RT_RCDATA);
  try
    SetLength(Result, ResStream.Size);
    ResStream.ReadBuffer(Pointer(Result)^, ResStream.Size);
  finally
    ResStream.Free;
  end;
end;


// 등록된 리소스 파싱하여 사용
var
  JSONString: string;
  JSONData: TJSONData; // 또는 사용하는 JSON 객체 타입
begin
  // 리소스에서 JSON 문자열 로드
  JSONString := LoadJSONFromResource('DATA_JSON'); // 리소스 이름 지정

  // 기존에 구현된 파싱 함수 사용 (예시)
  JSONData := YourParsingFunction(JSONString);

  try
    // JSON 데이터 활용 예시
    if JSONData <> nil then
    begin
      ShowMessage('JSON 로드 성공: ' + JSONData.FormatJSON);
      // 원하는 데이터 처리...
    end;
  finally
    JSONData.Free;
  end;
end;

