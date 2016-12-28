$ErrorActionPreference = 'Stop'

$SolutionDirectory = Split-Path -Path $PSScriptRoot -Parent

$CoverageResultsFilename = Join-Path $SolutionDirectory coverageresults.xml
$ThresholdsFilename = Join-Path $SolutionDirectory thresholds.xml

function Convert-ToNumber($n)
{
    0 + $n
}

function Get-Thresholds($Filename)
{
    $result = @{}
    [xml]$XmlDocument = Get-Content -Path $Filename
    $result.Add("CyclomaticComplexity", (Convert-ToNumber $XmlDocument.BuildThresholds.CyclomaticComplexity))
    $result.Add("BranchCoverage", (Convert-ToNumber $XmlDocument.BuildThresholds.BranchCoverage))
    $result.Add("SequenceCoverage", (Convert-ToNumber $XmlDocument.BuildThresholds.SequenceCoverage))
    $result
}

function Get-CoverageResults($Filename)
{
    $result = @{}
    [xml]$XmlDocument = Get-Content -Path $Filename
    $result.Add("CyclomaticComplexity", (Convert-ToNumber $XmlDocument.CoverageSession.Summary.maxCyclomaticComplexity))
    $result.Add("BranchCoverage", (Convert-ToNumber $XmlDocument.CoverageSession.Summary.branchCoverage))
    $result.Add("SequenceCoverage", (Convert-ToNumber $XmlDocument.CoverageSession.Summary.sequenceCoverage))
    $result
}

if (-Not (Test-Path $CoverageResultsFilename))
{
    throw "Missing coverage results at $($CoverageResultsFilename)."
}

if (-Not (Test-Path $ThresholdsFilename))
{
    throw "Missing thresholds at $($ThresholdsFilename)."
}

$Thresholds = Get-Thresholds $ThresholdsFilename
$ActualLevels = Get-CoverageResults $CoverageResultsFilename

$Passed = $True
if ($ActualLevels.CyclomaticComplexity -gt $Thresholds.CyclomaticComplexity)
{
    $Passed = $False
    Write-Host "Failure: Actual CyclomaticComplexity ($($ActualLevels.CyclomaticComplexity)) is above the threshold ($($Thresholds.CyclomaticComplexity))"
}
else
{
    Write-Host "Success: Actual CyclomaticComplexity ($($ActualLevels.CyclomaticComplexity)) is below the threshold ($($Thresholds.CyclomaticComplexity))"
}

if ($ActualLevels.SequenceCoverage -lt $Thresholds.SequenceCoverage)
{
    $Passed = $False
    Write-Host "Failure: Actual SequenceCoverage ($($ActualLevels.SequenceCoverage)) is below the threshold ($($Thresholds.SequenceCoverage))"
}
else
{
    Write-Host "Success: Actual SequenceCoverage ($($ActualLevels.SequenceCoverage)) is above the threshold ($($Thresholds.SequenceCoverage))"
}

if ($ActualLevels.BranchCoverage -lt $Thresholds.BranchCoverage)
{
    $Passed = $False
    Write-Host "Failure: Actual BranchCoverage ($($ActualLevels.BranchCoverage)) is below the threshold ($($Thresholds.BranchCoverage))"
}
else
{
    Write-Host "Success: Actual BranchCoverage ($($ActualLevels.BranchCoverage)) is above the threshold ($($Thresholds.BranchCoverage))"
}

if (-Not $Passed)
{
    throw "HygieneFailure"
}

Write-Host "HygieneSuccess: all checks passed!"
