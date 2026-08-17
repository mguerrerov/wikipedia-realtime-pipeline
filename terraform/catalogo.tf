# Glue Data Catalog. Equivale al catalogo de ficheros que usa Iceberg en local.
#
# Aqui esta la unica diferencia de fondo entre los dos entornos en la capa de
# almacenamiento: en local los metadatos de Iceberg viven junto a los datos, y
# en AWS los lleva Glue. La absorbe `src/almacenamiento.py`, no los jobs.
#
# Coste: el catalogo es gratis hasta el primer millon de objetos almacenados y
# el primer millon de peticiones al mes. Este proyecto tendra tres bases y un
# punado de tablas.

resource "aws_glue_catalog_database" "bronce" {
  name        = "${var.prefijo}_bronce"
  description = "Eventos crudos, tal y como llegan de la cola"

  location_uri = "s3://${aws_s3_bucket.almacen.id}/warehouse/bronce"
}

resource "aws_glue_catalog_database" "plata" {
  name        = "${var.prefijo}_plata"
  description = "Eventos tipados y deduplicados"

  location_uri = "s3://${aws_s3_bucket.almacen.id}/warehouse/plata"
}

resource "aws_glue_catalog_database" "oro" {
  name        = "${var.prefijo}_oro"
  description = "Agregaciones por ventana: las tres preguntas del pipeline"

  location_uri = "s3://${aws_s3_bucket.almacen.id}/warehouse/oro"
}
