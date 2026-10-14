[CmdletBinding()]
param(
    [string]$Fund = "EHP Alpha",
    [string]$Date = "20261013"
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($Fund) -or $Date -notmatch '^\d{8}$') {
    throw "Provide a non-empty fund and a date in YYYYMMDD format."
}
[void][datetime]::ParseExact($Date, "yyyyMMdd", [Globalization.CultureInfo]::InvariantCulture)

$connection = [System.Data.Odbc.OdbcConnection]::new("DSN=PSC_VIEWER")
try {
    $connection.Open()
    $command = $connection.CreateCommand()
    $command.CommandText = @"
SELECT COUNT(*) AS position_rows,
       COUNT(DISTINCT PORTFOLIO_NAV) AS distinct_navs,
       MIN(PORTFOLIO_NAV) AS min_nav,
       MAX(PORTFOLIO_NAV) AS max_nav
FROM psc_position_history
WHERE PORTFOLIO = ? AND POSN_DATE_INT = ?
"@
    $fundParameter = $command.Parameters.Add("fund", [System.Data.Odbc.OdbcType]::VarChar)
    $fundParameter.Value = $Fund
    $dateParameter = $command.Parameters.Add("date", [System.Data.Odbc.OdbcType]::Int)
    $dateParameter.Value = [int]$Date
    $reader = $command.ExecuteReader()
    try {
        if (-not $reader.Read()) { throw "PSC returned no summary row." }
        [pscustomobject]@{
            Fund = $Fund
            Date = $Date
            PositionRows = $reader.GetValue(0)
            DistinctNavValues = $reader.GetValue(1)
            MinNav = $reader.GetValue(2)
            MaxNav = $reader.GetValue(3)
        }
    } finally {
        $reader.Dispose()
    }
} finally {
    $connection.Dispose()
}
