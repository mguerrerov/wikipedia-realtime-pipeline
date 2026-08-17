variable "perfil" {
  description = "Perfil de AWS al que se despliega. Obligatorio y sin valor por defecto: nada debe apuntar por accidente a la cuenta que tenga configurada el CLI."
  type        = string

  validation {
    condition     = length(trimspace(var.perfil)) > 0
    error_message = "Hay que indicar un perfil de AWS explicitamente."
  }
}

variable "region" {
  description = "Region de AWS. Estocolmo por defecto: es la que ya tiene configurada el CLI y esta entre las mas baratas."
  type        = string
  default     = "eu-north-1"
}

variable "prefijo" {
  description = "Prefijo de todos los nombres de recurso. Sirve para localizarlos y para borrarlos sin dudar."
  type        = string
  default     = "wikipipe"
}

variable "retencion_horas" {
  description = "Horas que Kinesis guarda los registros. 24 es el minimo y el mas barato; ampliarlo se factura aparte."
  type        = number
  default     = 24

  validation {
    condition     = var.retencion_horas >= 24 && var.retencion_horas <= 168
    error_message = "Kinesis admite entre 24 y 168 horas sin coste de retencion extendida."
  }
}

variable "shards" {
  description = "Shards del stream. Uno aguanta 1 MB/s de entrada; la fuente real produce 53 KB/s, asi que sobra."
  type        = number
  default     = 1

  validation {
    condition     = var.shards >= 1 && var.shards <= 4
    error_message = "Mas de 4 shards no tiene sentido en este proyecto y multiplica el coste por hora."
  }
}

variable "dias_expiracion_checkpoints" {
  description = "Dias tras los que se borran los puntos de control en S3. Medido en local: ocupan 13 veces mas que los datos."
  type        = number
  default     = 7
}

variable "capacidad_maxima_vcpu" {
  description = "Tope de vCPU que EMR Serverless puede llegar a usar. Es el freno de coste principal."
  type        = number
  default     = 8
}

variable "capacidad_maxima_memoria_gb" {
  description = "Tope de memoria en GB para EMR Serverless."
  type        = number
  default     = 32
}

variable "minutos_parada_automatica" {
  description = "Minutos de inactividad tras los que EMR Serverless se apaga solo. No para un job de streaming vivo: solo evita pagar por una aplicacion ociosa."
  type        = number
  default     = 15
}

variable "correo_avisos" {
  description = "Correo al que llegan los avisos de gasto. Obligatorio: sin destinatario, el presupuesto no sirve de nada."
  type        = string

  validation {
    condition     = can(regex("^[^@ ]+@[^@ ]+[.][^@ ]+$", var.correo_avisos))
    error_message = "Hay que indicar un correo valido para los avisos de gasto."
  }
}

variable "tope_gasto_usd" {
  description = "Tope mensual de gasto en dolares que dispara los avisos. El presupuesto del proyecto entero es de menos de 15 EUR."
  type        = string
  default     = "10"
}
