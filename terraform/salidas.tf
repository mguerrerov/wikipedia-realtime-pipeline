# Lo que hace falta para lanzar el job y para comprobar que se ha destruido
# todo. Se imprime con `terraform output`.

output "bucket_almacen" {
  description = "Bucket del almacen Iceberg"
  value       = aws_s3_bucket.almacen.id
}

output "ruta_almacen" {
  description = "Valor de la variable de entorno ALMACEN en AWS"
  value       = "s3://${aws_s3_bucket.almacen.id}/warehouse"
}

output "stream_kinesis" {
  description = "Valor de la variable de entorno KINESIS_STREAM"
  value       = aws_kinesis_stream.cambios.name
}

output "aplicacion_emr" {
  description = "Identificador de la aplicacion de EMR Serverless"
  value       = aws_emrserverless_application.spark.id
}

output "rol_ejecucion" {
  description = "ARN del rol que se pasa a cada job"
  value       = aws_iam_role.ejecucion_emr.arn
}

output "variables_de_entorno" {
  description = "Bloque listo para pegar al lanzar los jobs contra AWS"
  value = join("\n", [
    "FUENTE_EVENTOS=kinesis",
    "CATALOGO=glue",
    "KINESIS_STREAM=${aws_kinesis_stream.cambios.name}",
    "AWS_REGION=${var.region}",
    "ALMACEN=s3://${aws_s3_bucket.almacen.id}/warehouse",
  ])
}

output "recursos_a_verificar_tras_destruir" {
  description = "Lista para la comprobacion manual posterior al destroy"
  value = join("\n", [
    "s3       ${aws_s3_bucket.almacen.id}",
    "kinesis  ${aws_kinesis_stream.cambios.name}",
    "emr      ${aws_emrserverless_application.spark.id}",
    "iam      ${aws_iam_role.ejecucion_emr.name}",
    "glue     ${aws_glue_catalog_database.bronce.name}, ${aws_glue_catalog_database.plata.name}, ${aws_glue_catalog_database.oro.name}",
  ])
}
