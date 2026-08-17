# Versiones fijadas, igual que en el Compose. Ver docs/versiones.md.
#
# El proveedor va con `=` y no con `~>`: un cambio de menor en el proveedor de
# AWS puede alterar un plan sin que yo toque nada, y en este proyecto un plan
# que cambia solo es un plan en el que no puedo confiar.

terraform {
  required_version = "= 1.15.8"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.60.0"
    }
  }
}

provider "aws" {
  region = var.region

  # El perfil se declara a proposito, sin valor por defecto util: asi ningun
  # comando puede acabar apuntando por accidente a la cuenta que el CLI tenga
  # configurada. Hay que decir a que cuenta se va, cada vez.
  profile = var.perfil

  # Toda etiqueta obligatoria del proyecto, aplicada a cualquier recurso que
  # las admita. Puesto aqui y no recurso a recurso para que no se olvide
  # ninguno: si no aparece la etiqueta, no lo hemos creado nosotros.
  default_tags {
    tags = {
      project  = "streaming-portfolio"
      entorno  = "validacion"
      gestion  = "terraform"
      proyecto = "wikipedia-realtime-pipeline"
    }
  }
}
