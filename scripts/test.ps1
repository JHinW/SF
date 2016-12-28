$ErrorActionPreference = 'Stop'

function HashValuesToString($ht)
{
    $l = @()
    foreach($h in $ht.GetEnumerator())
    {
        $l += $h.Value
    }
    $l -join " "
}

function Get-DllPaths($DllsHash)
{
    $searchPaths = @()
    foreach($h in $dlls.GetEnumerator())
    {
        $searchPaths += (Split-Path $h.Value)
    }
    $searchPaths -join ";"
}

$SolutionDirectory = Split-Path -Path $PSScriptRoot -Parent
$dlls = @{}
Write-Host "Looking in $SolutionDirectory"
Get-ChildItem -Path "$SolutionDirectory" -Filter "*Test*.dll" -Recurse | Where-object { !($_.Name -like "Microsoft.VisualStudio.QualityTools*") -And !( $_.FullName -like "*xunit*") -And ($_.FullName -like "*bin\x64\Debug*")} | % { $dlls[$_.Name] = (Resolve-Path -Relative $_.FullName) }

$dllsString = HashValuesToString($dlls)
Write-Host "Found these libraries to test:"
$dllsString | format-list

$openCoverPath = Join-Path $SolutionDirectory packages\OpenCover.4.6.519\tools\OpenCover.Console.exe
$xunitPath = Join-Path $SolutionDirectory packages\xunit.runner.console.2.1.0\tools\xunit.console.exe
$searchPathString = Get-DllPaths $dlls

& "$openCoverPath" `
 "-register:user" `
 "-target:$xunitPath" `
 "-targetargs:$dllsString -xml testresults.xml" `
 "-output:coverageresults.xml" `
 "-filter:+[EventHubLogProcessor]*" `
 "-searchdirs:$searchPathString"

