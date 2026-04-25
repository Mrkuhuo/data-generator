param(
    [ValidateSet("MYSQL", "POSTGRESQL", "SQLSERVER", "ORACLE", "KAFKA")]
    [string]$DbType,
    [string]$TargetHost = "127.0.0.1",
    [int]$Port,
    [string]$DatabaseName,
    [string]$SchemaName = "",
    [string]$Username,
    [string]$Password,
    [string]$JdbcParams = "",
    [string]$ApiBaseUrl = "http://127.0.0.1:8888/api",
    [string]$TableName = "",
    [int]$RowCount = 5,
    [string]$KafkaBootstrapServers = "",
    [string]$KafkaConfigJson = "",
    [ValidateSet("NONE", "FIELD", "FIXED")]
    [string]$KafkaKeyMode = "NONE",
    [string]$KafkaKeyField = "order_id",
    [string]$KafkaFixedKey = "",
    [string]$KafkaHeadersJson = "",
    [switch]$KeepResources
)

$ErrorActionPreference = "Stop"

function Test-IsKafka {
    param([string]$Type)

    return $Type -eq "KAFKA"
}

function Get-DefaultPort {
    param([string]$Type)

    switch ($Type) {
        "MYSQL" { return 3306 }
        "POSTGRESQL" { return 5432 }
        "SQLSERVER" { return 1433 }
        "ORACLE" { return 1521 }
        "KAFKA" { return 9092 }
        default { throw "Unsupported db type: $Type" }
    }
}

function Get-DefaultJdbcParams {
    param([string]$Type)

    switch ($Type) {
        "MYSQL" { return "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai" }
        "SQLSERVER" { return "encrypt=true;trustServerCertificate=true" }
        default { return "" }
    }
}

function Get-DateColumnType {
    param([string]$Type)

    switch ($Type) {
        "MYSQL" { return "DATETIME" }
        "SQLSERVER" { return "DATETIME2" }
        default { return "TIMESTAMP" }
    }
}

function Use-SchemaQualifiedTableName {
    param([string]$Type)

    return $Type -in @("POSTGRESQL", "SQLSERVER", "ORACLE")
}

function Get-KafkaBootstrapServers {
    if ($KafkaBootstrapServers) {
        return $KafkaBootstrapServers
    }
    return "$TargetHost`:$Port"
}

function Read-JsonObject {
    param([string]$Json)

    if (-not $Json) {
        return $null
    }

    return $Json | ConvertFrom-Json
}

function Get-SmokeColumns {
    param([string]$Type)

    $dateColumnType = Get-DateColumnType -Type $Type
    return @(
        @{
            columnName = "order_id"
            dbType = "BIGINT"
            lengthValue = $null
            precisionValue = $null
            scaleValue = $null
            nullableFlag = $false
            primaryKeyFlag = $true
            generatorType = "SEQUENCE"
            generatorConfig = @{ start = 1; step = 1 }
            sortOrder = 0
        },
        @{
            columnName = "customer_name"
            dbType = "VARCHAR"
            lengthValue = 64
            precisionValue = $null
            scaleValue = $null
            nullableFlag = $false
            primaryKeyFlag = $false
            generatorType = "STRING"
            generatorConfig = @{
                mode = "random"
                length = 6
                charset = "abcdefghijklmnopqrstuvwxyz0123456789"
                prefix = "USR-"
                suffix = ""
            }
            sortOrder = 1
        },
        @{
            columnName = "amount"
            dbType = "DECIMAL"
            lengthValue = $null
            precisionValue = 10
            scaleValue = 2
            nullableFlag = $false
            primaryKeyFlag = $false
            generatorType = "RANDOM_DECIMAL"
            generatorConfig = @{
                min = 10
                max = 999
                scale = 2
            }
            sortOrder = 2
        },
        @{
            columnName = "created_at"
            dbType = $dateColumnType
            lengthValue = $null
            precisionValue = $null
            scaleValue = $null
            nullableFlag = $false
            primaryKeyFlag = $false
            generatorType = "DATETIME"
            generatorConfig = @{
                from = "2026-04-01T00:00:00Z"
                to = "2026-04-30T23:59:59Z"
            }
            sortOrder = 3
        }
    )
}

function Get-ConnectionBody {
    param(
        [string]$Type,
        [string]$Timestamp
    )

    if (Test-IsKafka -Type $Type) {
        $config = if ($KafkaConfigJson) {
            Read-JsonObject -Json $KafkaConfigJson
        } else {
            [ordered]@{
                bootstrapServers = Get-KafkaBootstrapServers
                clientId = "mdg-smoke-$Timestamp"
                acks = "all"
            }
        }

        return @{
            name = "Smoke $Type $Timestamp"
            dbType = $Type
            host = $TargetHost
            port = $Port
            databaseName = if ($DatabaseName) { $DatabaseName } else { "kafka" }
            schemaName = $null
            username = if ($Username) { $Username } else { "" }
            password = if ($Password) { $Password } else { "" }
            jdbcParams = $null
            configJson = ($config | ConvertTo-Json -Depth 20 -Compress)
            status = "READY"
            description = "Automated smoke test connection"
        }
    }

    return @{
        name = "Smoke $Type $Timestamp"
        dbType = $Type
        host = $TargetHost
        port = $Port
        databaseName = $DatabaseName
        schemaName = if ($SchemaName) { $SchemaName } else { $null }
        username = $Username
        password = $Password
        jdbcParams = if ($JdbcParams) { $JdbcParams } else { $null }
        status = "READY"
        description = "Automated smoke test connection"
    }
}

function Get-TaskBody {
    param(
        [string]$Type,
        [pscustomobject]$Connection,
        [string]$TargetName,
        [string]$Timestamp
    )

    $columns = Get-SmokeColumns -Type $Type
    if (Test-IsKafka -Type $Type) {
        $headers = if ($KafkaHeadersJson) { Read-JsonObject -Json $KafkaHeadersJson } else { $null }
        $targetConfig = [ordered]@{
            payloadFormat = "JSON"
            keyMode = $KafkaKeyMode
        }

        switch ($KafkaKeyMode) {
            "FIELD" {
                $targetConfig.keyField = $KafkaKeyField
            }
            "FIXED" {
                if (-not $KafkaFixedKey) {
                    throw "KafkaFixedKey is required when KafkaKeyMode=FIXED"
                }
                $targetConfig.fixedKey = $KafkaFixedKey
            }
        }

        if ($headers) {
            $targetConfig.headers = $headers
        }

        return @{
            name = "Smoke Task $Type $Timestamp"
            connectionId = $Connection.id
            tableName = $TargetName
            tableMode = "USE_EXISTING"
            writeMode = "APPEND"
            rowCount = $RowCount
            batchSize = [Math]::Max($RowCount, 1)
            seed = 20260416
            status = "READY"
            scheduleType = "MANUAL"
            cronExpression = $null
            triggerAt = $null
            intervalSeconds = $null
            maxRuns = $null
            maxRowsTotal = $null
            description = "Automated smoke test task"
            targetConfigJson = ($targetConfig | ConvertTo-Json -Depth 20 -Compress)
            columns = $columns
        }
    }

    return @{
        name = "Smoke Task $Type $Timestamp"
        connectionId = $Connection.id
        tableName = $TargetName
        tableMode = "CREATE_IF_MISSING"
        writeMode = "OVERWRITE"
        rowCount = $RowCount
        batchSize = [Math]::Max($RowCount, 1)
        seed = 20260416
        status = "READY"
        scheduleType = "MANUAL"
        cronExpression = $null
        triggerAt = $null
        intervalSeconds = $null
        maxRuns = $null
        maxRowsTotal = $null
        description = "Automated smoke test task"
        targetConfigJson = $null
        columns = $columns
    }
}

function Get-HeaderCount {
    param($Headers)

    if ($null -eq $Headers) {
        return 0
    }
    if ($Headers -is [System.Collections.IDictionary]) {
        return $Headers.Count
    }
    return $Headers.PSObject.Properties.Count
}

function Invoke-Api {
    param(
        [string]$Method,
        [string]$Path,
        $Body = $null
    )

    $request = @{
        Uri = "$ApiBaseUrl$Path"
        Method = $Method
        TimeoutSec = 60
    }

    if ($null -ne $Body) {
        $request.ContentType = "application/json"
        $request.Body = ($Body | ConvertTo-Json -Depth 20 -Compress)
    }

    try {
        $response = Invoke-RestMethod @request
    } catch {
        $httpResponse = $_.Exception.Response
        if ($httpResponse -and $httpResponse.GetResponseStream()) {
            $reader = New-Object System.IO.StreamReader($httpResponse.GetResponseStream())
            $bodyText = $reader.ReadToEnd()
            throw "API request failed: $Method $Path`n$bodyText"
        }
        throw
    }

    if (-not $response.success) {
        throw "API request failed: $Method $Path`n$($response.message)"
    }

    return $response
}

if (-not $Port) {
    $Port = Get-DefaultPort -Type $DbType
}

if (-not (Test-IsKafka -Type $DbType) -and -not $JdbcParams) {
    $JdbcParams = Get-DefaultJdbcParams -Type $DbType
}

$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$baseTableName = if ($TableName) { $TableName } else { "mdg_smoke_$($DbType.ToLowerInvariant())_$timestamp" }
$qualifiedTableName = if ($SchemaName -and (Use-SchemaQualifiedTableName -Type $DbType)) {
    "$SchemaName.$baseTableName"
} else {
    $baseTableName
}

$artifacts = @()

try {
    $connectionResponse = Invoke-Api -Method "Post" -Path "/connections" -Body (Get-ConnectionBody -Type $DbType -Timestamp $timestamp)
    $connection = $connectionResponse.data
    $artifacts += @{ kind = "connection"; id = $connection.id }

    $connectionTest = (Invoke-Api -Method "Post" -Path "/connections/$($connection.id)/test").data
    if (-not $connectionTest.success) {
        throw "Connection test failed: $($connectionTest.message)"
    }

    $taskResponse = Invoke-Api -Method "Post" -Path "/write-tasks" -Body (
        Get-TaskBody -Type $DbType -Connection $connection -TargetName $qualifiedTableName -Timestamp $timestamp
    )
    $task = $taskResponse.data
    $artifacts += @{ kind = "task"; id = $task.id }

    $execution = (Invoke-Api -Method "Post" -Path "/write-tasks/$($task.id)/run").data
    $deliveryDetails = if ($execution.deliveryDetailsJson) {
        $execution.deliveryDetailsJson | ConvertFrom-Json
    } else {
        $null
    }

    if (Test-IsKafka -Type $DbType) {
        [ordered]@{
            dbType = $DbType
            connectionId = $connection.id
            taskId = $task.id
            executionId = $execution.id
            topic = $qualifiedTableName
            connectionStatus = $connectionTest.status
            executionStatus = $execution.status
            writtenRowCount = if ($deliveryDetails) { [int64]$deliveryDetails.writtenRowCount } else { 0 }
            errorCount = if ($deliveryDetails) { [int64]$deliveryDetails.errorCount } else { 0 }
            payloadFormat = if ($deliveryDetails) { $deliveryDetails.payloadFormat } else { $null }
            keyMode = if ($deliveryDetails) { $deliveryDetails.keyMode } else { $KafkaKeyMode }
            keyField = if ($deliveryDetails) { $deliveryDetails.keyField } else { $null }
            fixedKey = if ($deliveryDetails) { $deliveryDetails.fixedKey } else { $null }
            partition = if ($deliveryDetails) { $deliveryDetails.partition } else { $null }
            headerCount = if ($deliveryDetails) { Get-HeaderCount -Headers $deliveryDetails.headers } else { 0 }
            nullValueCount = if ($deliveryDetails) { [int64]$deliveryDetails.nonNullValidation.nullValueCount } else { 0 }
            blankStringCount = if ($deliveryDetails) { [int64]$deliveryDetails.nonNullValidation.blankStringCount } else { 0 }
        } | ConvertTo-Json -Depth 10
    } else {
        $tables = (Invoke-Api -Method "Get" -Path "/connections/$($connection.id)/tables").data
        $encodedTableName = [uri]::EscapeDataString($qualifiedTableName)
        $columns = (Invoke-Api -Method "Get" -Path "/connections/$($connection.id)/table-columns?tableName=$encodedTableName").data

        [ordered]@{
            dbType = $DbType
            connectionId = $connection.id
            taskId = $task.id
            executionId = $execution.id
            tableName = $qualifiedTableName
            connectionStatus = $connectionTest.status
            executionStatus = $execution.status
            listedTableCount = @($tables).Count
            importedColumnCount = @($columns).Count
            writtenRowCount = if ($deliveryDetails) { [int64]$deliveryDetails.writtenRowCount } else { 0 }
            beforeWriteRowCount = if ($deliveryDetails) { [int64]$deliveryDetails.beforeWriteRowCount } else { 0 }
            afterWriteRowCount = if ($deliveryDetails) { [int64]$deliveryDetails.afterWriteRowCount } else { 0 }
            rowDelta = if ($deliveryDetails) { [int64]$deliveryDetails.rowDelta } else { 0 }
            nullValueCount = if ($deliveryDetails) { [int64]$deliveryDetails.nonNullValidation.nullValueCount } else { 0 }
            blankStringCount = if ($deliveryDetails) { [int64]$deliveryDetails.nonNullValidation.blankStringCount } else { 0 }
        } | ConvertTo-Json -Depth 10
    }
}
finally {
    if (-not $KeepResources) {
        foreach ($artifact in ($artifacts | Sort-Object kind -Descending)) {
            try {
                if ($artifact.kind -eq "task") {
                    Invoke-Api -Method "Delete" -Path "/write-tasks/$($artifact.id)" | Out-Null
                } elseif ($artifact.kind -eq "connection") {
                    Invoke-Api -Method "Delete" -Path "/connections/$($artifact.id)" | Out-Null
                }
            } catch {
            }
        }
    }
}
