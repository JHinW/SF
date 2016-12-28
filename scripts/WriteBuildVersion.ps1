[CmdletBinding()]
Param(
	[Parameter(Mandatory=$True)]
	[string]$revision
);

function SetApplicationManifestVersion($filePath, $version)
{
	Write-Host "Setting version to $version in file $filePath"
	
	$appManifest = New-Object XML;
	$appManifest.Load($filePath);
	
	# Update version values
	$appManifest.ApplicationManifest.ApplicationTypeVersion = $version;
	foreach ($serviceManifestImport in $appManifest.ApplicationManifest.ServiceManifestImport)
	{
		$serviceManifestImport.ServiceManifestRef.ServiceManifestVersion = $version;
	}
	
	$appManifest.Save($filePath);
}

function SetServiceManifestVersion($filePath, $version)
{
	Write-Host "Setting version to $version in file $filePath"
	
	$svcManifest = New-Object XML;
	$svcManifest.Load($filePath);
	
	# Update version values
	$svcManifest.ServiceManifest.Version = $version;
	$svcManifest.ServiceManifest.CodePackage.Version = $version;
	$svcManifest.ServiceManifest.ConfigPackage.Version = $version;
	
	$svcManifest.Save($filePath);
}

# The root of the project
$projectRoot = Split-Path $PSScriptRoot;

# Create the version number for this build
$metadata = New-Object XML;
$metadata.Load((Join-Path $projectRoot "metadata.xml"));
$version = $metadata.metadata.version + "." + $revision;

# Get the filepath of all the files we want to update the version in
$appManifest = Join-Path $projectRoot "EventHubLogProcessorApp\ApplicationPackageRoot\ApplicationManifest.xml";
$processorSvcManifest = Join-Path $projectRoot "EventHubLogProcessor.ProcessorSvc\PackageRoot\ServiceManifest.xml";
$telemetrySvcManifest = Join-Path $projectRoot "EventHubLogProcessor.TelemetrySvc\PackageRoot\ServiceManifest.xml";

# Set the version number in all the files
SetApplicationManifestVersion $appManifest $version;
SetServiceManifestVersion $processorSvcManifest $version;
SetServiceManifestVersion $telemetrySvcManifest $version;
