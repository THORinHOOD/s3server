# S3 Java server

## Как запустить

> java -jar <>.jar


> Флаги \
--port=9090 (порт, по которому будет работать сервер)\
--basePath=путь (путь до основной папки, где будут храниться бакеты и объекты) \
--users=пути (пути до json файлов через запятую, где описаны пользователи)

> Пример json файла пользователя \
{ \
"accessKey" : <ключ по которому определяется пользователь>, \
"secretKey" : <серкетный ключ, которым будет подписываться каждый запрос>, \
"userId" : "504587744049" <id пользователя>, \
"userName" : <если это IAM пользователь>, \
"arn" : "arn:aws:iam::504587744049:<root или user/userName", \
"canonicalUserId" : <каноничный id пользователя, используется в ACL>, \
"accountName" : <имя основного аккаунта> \
}

## Методы

> ###Реализованные методы:
> * GetBucketAcl
> * GetObjectAcl
> * PutBucketAcl
> * PutObjectAcl
> * CopyObject
> * CreateBucket
> * DeleteBucket
> * DeleteObject
> * GetObject
> * PutObject
> * ListBuckets
> * ListObjects
> * ListObjectsV2
> * AbortMultipartUpload
> * CompleteMultipartUpload
> * CreateMultipartUpload
> * UploadPart
> * GetBucketPolicy
> * PutBucketPolicy
> * HeadObject