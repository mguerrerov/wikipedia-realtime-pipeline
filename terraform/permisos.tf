# Rol que asume el job de EMR Serverless al ejecutarse.
#
# Los permisos van acotados a los recursos concretos de este proyecto, no con
# comodines. No es celo de seguridad: un rol amplio es un rol que puede tocar
# cosas que `terraform destroy` no va a limpiar, y eso choca con la regla de
# que todo debe desaparecer con un solo comando.

data "aws_iam_policy_document" "asumir_emr" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    # Impide que otra cuenta pueda usar este rol aunque conozca su ARN.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.actual.account_id]
    }
  }
}

resource "aws_iam_role" "ejecucion_emr" {
  name               = "${var.prefijo}-ejecucion-emr"
  assume_role_policy = data.aws_iam_policy_document.asumir_emr.json
}

data "aws_iam_policy_document" "permisos_job" {
  # --- Almacen de objetos ---
  statement {
    sid    = "LeerYEscribirElAlmacen"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.almacen.arn}/*"]
  }

  statement {
    sid    = "ListarElAlmacen"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.almacen.arn]
  }

  # --- Cola ---
  # Solo lectura: quien escribe en Kinesis es el productor, que corre fuera de
  # EMR y usa sus propias credenciales.
  statement {
    sid    = "ConsumirDeKinesis"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:SubscribeToShard",
      "kinesis:RegisterStreamConsumer",
      "kinesis:DeregisterStreamConsumer",
      "kinesis:DescribeStreamConsumer",
      "kinesis:ListStreamConsumers",
    ]
    resources = [
      aws_kinesis_stream.cambios.arn,
      "${aws_kinesis_stream.cambios.arn}/*",
    ]
  }

  # --- Catalogo ---
  statement {
    sid    = "UsarGlue"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:BatchCreatePartition",
      "glue:BatchGetPartition",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.actual.account_id}:catalog",
      aws_glue_catalog_database.bronce.arn,
      aws_glue_catalog_database.plata.arn,
      aws_glue_catalog_database.oro.arn,
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.actual.account_id}:table/${var.prefijo}_*/*",
    ]
  }
}

resource "aws_iam_role_policy" "permisos_job" {
  name   = "${var.prefijo}-permisos-job"
  role   = aws_iam_role.ejecucion_emr.id
  policy = data.aws_iam_policy_document.permisos_job.json
}
