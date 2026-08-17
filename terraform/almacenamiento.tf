# Bucket del almacen Iceberg. Equivale a MinIO en local: las mismas rutas, el
# mismo cliente. Esa es la razon de haber elegido este stack.

data "aws_caller_identity" "actual" {}

locals {
  # Los nombres de bucket son globales en todo AWS, asi que se le pega el
  # identificador de cuenta y la region para que no choque con el de nadie.
  bucket = "${var.prefijo}-almacen-${data.aws_caller_identity.actual.account_id}-${var.region}"
}

resource "aws_s3_bucket" "almacen" {
  bucket = local.bucket

  # Imprescindible para la regla de "todo destruible con un solo destroy": sin
  # esto, `terraform destroy` falla si el bucket tiene objetos dentro, y este
  # va a tenerlos por definicion. Es peligroso en produccion y correcto aqui.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "almacen" {
  bucket = aws_s3_bucket.almacen.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "almacen" {
  bucket = aws_s3_bucket.almacen.id

  rule {
    apply_server_side_encryption_by_default {
      # SSE-S3 y no KMS: KMS cobra por peticion, y un job de streaming hace
      # muchisimas peticiones pequenas.
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "almacen" {
  bucket = aws_s3_bucket.almacen.id

  # Los puntos de control ocupan mucho mas que los datos: medido en local, 180
  # MB frente a 13,7 MB en ocho minutos de ejecucion. Sin esta regla, el
  # almacenamiento del proyecto lo dominaria el estado de Spark, no los datos.
  rule {
    id     = "expirar-checkpoints"
    status = "Enabled"

    filter {
      prefix = "warehouse/_checkpoints/"
    }

    expiration {
      days = var.dias_expiracion_checkpoints
    }
  }

  # Los ficheros que Iceberg deja huerfanos tras una compactacion o un
  # reintento no se borran solos. Aqui no hay versionado, pero si subidas
  # incompletas si un job muere a mitad de escritura.
  rule {
    id     = "limpiar-subidas-incompletas"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
