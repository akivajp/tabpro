FILE_FIELD = '__file__'
ROW_INDEX_FIELD = '__row_index__'
FILE_ROW_INDEX_FIELD = '__file_row_index__'
INPUT_FIELD = '__input__'
STAGING_FIELD = '__staging__'
# NOTE:
#   validate が付与する違反情報の格納先。
#   __staging__ が「出力前に捨てられる中間値」の領域であるのに対し、
#   こちらは「捨ててはならない検査結果」であるため独立させている。
VIOLATIONS_FIELD = '__violations__'
