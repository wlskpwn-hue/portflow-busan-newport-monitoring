# 공개 저장소 설정 메모

이 저장소는 공개 업로드용으로 민감한 정보가 제거된 상태입니다.

## 실행 전에 바꿔야 할 값

- `AZURE_STORAGE_ACCOUNT`
- `AZURE_STORAGE_CONTAINER`
- `AZURE_STORAGE_CONTAINER_OPERATION`
- `AZURE_STORAGE_CONTAINER_RAW`
- `AZURE_SQL_HOST`
- `AZURE_SQL_DATABASE`
- `AZURE_SQL_USER`
- `DATABRICKS_SECRET_SCOPE`
- `DATABRICKS_SECRET_KEY`
- `DATABRICKS_CATALOG`

## 참고 사항

- 실제 비밀번호나 토큰은 코드에 직접 넣지 마세요.
- 비밀번호는 Databricks secret scope 같은 비밀 저장소에서 불러오세요.
- `tmp_excel/`, `bronze_staging/` 같은 로컬 산출물은 Git에 올리지 마세요.
