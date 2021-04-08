# S3 server

## Как запустить

> java -jar <>.jar

> Флаги \
--port=9090 (порт, по которому будет работать сервер)\
--basePath=путь (путь до основной папки, где будут храниться бакеты и объекты) \
> --dbPath=путь (путь до папки, где будет храниться *.mv.db *.trace.db файлы базы данных)\
> --dbUser=user (имя пользователя бд)\
> --dbPassword=password (пароль от бд)

## Методы

> ### **PutObject**

PUT /< bucket >/< key to object > HTTP/1.1 \
Host: < server host >

**Необязательные заголовки, пристутствуют при запросе через AWS SDK** \
amz-sdk-invocation-id: 9d31ccb1-e17d-c49d-af3f-7f176ae6511d \
amz-sdk-request: attempt=1; max=3 \
=====\
Authorization: AWS4-HMAC-SHA256 \
Credential=< access key >/20210402/< region >/< service name >/aws4_request, \
SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-meta-kek, \
Signature=6fec029901a52d808f7062665e9a68241d374a73dedd9d1e554372fe9911f777 **(Signature Вычисляется по алгоритму https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html или https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html)**

Content-Type: text/plain \
Expect: 100-continue \
User-Agent: ... \
x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD **(если файл нужно разбить по чанкам)** / < payload hash > **(если файл нужно отправить одним целым чанком)**\
X-Amz-Date: 20210402T120933Z \
x-amz-decoded-content-length: 22 **(размер самого файла)**\
x-amz-meta-< name >: < value > **(метаданные)** \
Content-Length: 195 **(общий размер передаваемого)**\
Connection: Keep-Alive \


> ### **GetObject**
 
GET /< bucket >/< key to file > HTTP/1.1 \
Host: localhost:9090 

**Необязательные заголовки, пристутствуют при запросе через AWS SDK** \
amz-sdk-invocation-id: 63e0ca40-f647-76df-58a7-29b7950fcd8a\
amz-sdk-request: attempt=1; max=3\
======\
Authorization: AWS4-HMAC-SHA256 \
Credential=AKIAJQGYR4BXLYFNLMPA/20210407/us-west-2/s3/aws4_request, \
SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;host;x-amz-content-sha256;x-amz-date;x-amz-te,\
Signature=c8352e92f3bee1852e9d5ee817fafcc5fed0c6110dbd448d348239e05e355a33 \

User-Agent: ... \
x-amz-content-sha256: UNSIGNED-PAYLOAD \
X-Amz-Date: 20210407T133451Z \
x-amz-te: append-md5 \
Connection: Keep-Alive \
content-length: 0