$ErrorActionPreference = "Stop"

function Print-Help {
    Write-Output "Downloads the latest release of Datajure REPL if it is not found."
    Write-Output "Runs the Datajure REPL + starts an nREPL server with an .nrepl-port file."
    Write-Output "Usage:"
    Write-Output "`tdatajure [<path>] [--force-download] [--proxy <https-proxy>] [--help]"
    Write-Output "The optional input file should be a Datajure source file (.dtj suffix)."
}

$ScriptPath = $false
$ForceDownload = $false
$UseProxy = $false
$HttpsProxy = $false
for ($i = 0; $i -lt $args.count;) {
    $Key = $args[$i]
    switch -Wildcard ($Key) {
        "*.dtj" {
            $ScriptPath = $Key
            $i++
        }
        "--force-download" {
            $ForceDownload = $true
            $i++
        }
        "--proxy" {
            $UseProxy = $true
            $i++
            if ($i -ge $args.count) {
                Write-Output "Missing proxy server address!"
                Print-Help
                Exit 1
            }
            HttpsProxy = $aegs[$i]
            $i++
        }
        "--help" {
            Print-Help
            Exit 0
        }
        Default {
            Write-Output "Unrecognised option!"
            Print-Help
            Exit 1
        }
    }
}

function Get-LatestRelease($RepoName) {
    if ($UseProxy) {
        (Invoke-WebRequest "https://api.github.com/repos/$RepoName/releases/latest" -Proxy "$HttpsProxy").Content | ConvertFrom-Json | Select-Object -expand "tag_name"
    } else {
        (Invoke-WebRequest "https://api.github.com/repos/$RepoName/releases/latest").Content | ConvertFrom-Json | Select-Object -expand "tag_name"
    }
}

$RepoName = "clojure-finance/datajure"
$Version = get-LatestRelease($RepoName)
$OriginalDir = Get-Location | Foreach-Object { $_.Path }
$DatajureDir = "$HOME\.datajure"
$DownloadDir = "$DatajureDir\downloads"
$UberjarDownloadUrl = "https://github.com/$RepoName/releases/download/$Version/datajure-$Version-standalone.jar"
$WinutilsDownloadUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
$UberjarName = "datajure.jar"
$WinutilsName = "winutils.exe"
$JvmOpts = @(
    "-Dhadoop.home.dir=$DatajureDir"
    "--add-opens=java.base/java.nio=ALL-UNNAMED"
    "--add-opens=java.base/java.net=ALL-UNNAMED"
    "--add-opens=java.base/java.lang=ALL-UNNAMED"
    "--add-opens=java.base/java.util=ALL-UNNAMED"
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
) -join " "

if (!(Test-Path $DatajureDir -PathType Container)) {
    New-Item $DatajureDir -ItemType Directory
}
Set-Location -Path $DatajureDir
# download uberjar
if (!(Test-Path $UberjarName -PathType Leaf) -or $ForceDownload) {
    if (!(Test-Path $DownloadDir -PathType Container)) {
        New-Item $DownloadDir -ItemType Directory
    }
    Set-Location -Path $DownloadDir
    if ($UseProxy) {
        Write-Output "Downloading $UberjarDownloadUrl to $DownloadDir via proxy server $HttpsProxy ..."
        Invoke-WebRequest -OutFile $UberjarName -Uri $UberjarDownloadUrl -Proxy "$HttpsProxy"
    } else {
        Write-Output "Downloading $UberjarDownloadUrl to $DownloadDir ..."
        Invoke-WebRequest -OutFile $UberjarName -Uri $UberjarDownloadUrl
    }
    Write-Output "Moving $DownloadDir\$UberjarName to $DatajureDir\$UberjarName"
    Move-Item -Path "$UberjarName" -Destination "$DatajureDir\$UberjarName"
    Write-Output "Successfully downloaded Datajure REPL uberjar in $DatajureDir\$UberjarName"
}
# download winutils
if (!(Test-Path "bin\$WinutilsName" -PathType Leaf) -or $ForceDownload) {
    if (!(Test-Path $DownloadDir -PathType Container)) {
        New-Item $DownloadDir -ItemType Directory
    }
    Set-Location -Path $DownloadDir
    if ($UseProxy) {
        Write-Output "Downloading $WinutilsDownloadUrl to $DownloadDir via proxy server $HttpsProxy ..."
        Invoke-WebRequest -OutFile $WinutilsName -Uri $WinutilsDownloadUrl -Proxy "$HttpsProxy"
    } else {
        Write-Output "Downloading $WinutilsDownloadUrl to $DownloadDir ..."
        Invoke-WebRequest -OutFile $WinutilsName -Uri $WinutilsDownloadUrl
    }
    if (!(Test-Path "$DatajureDir\bin" -PathType Container)) {
        New-Item "$DatajureDir\bin" -ItemType Directory
    }
    Write-Output "Moving $DownloadDir\$WinutilsName to $DatajureDir\bin\$WinutilsName"
    Move-Item -Path "$WinutilsName" -Destination "$DatajureDir\bin\$WinutilsName"
    Write-Output "Successfully downloaded Hadoop Windows Utilities in $DatajureDir\bin\$WinutilsName"
}

Set-Location -Path $OriginalDir
if (!$ScriptPath) {
    cmd.exe /c "java $JvmOpts -jar $DatajureDir\$UberjarName"
} else {
    cmd.exe /c "java $JvmOpts -jar $DatajureDir\$UberjarName $ScriptPath"
}