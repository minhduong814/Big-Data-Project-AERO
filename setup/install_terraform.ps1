# PowerShell script to download and setup Terraform locally
$ErrorActionPreference = "Stop"

$TF_VERSION = "1.9.5"
$TF_URL = "https://releases.hashicorp.com/terraform/$TF_VERSION/terraform_${TF_VERSION}_windows_amd64.zip"
$DEST_DIR = "$PSScriptRoot\..\bin"
$ZIP_PATH = "$DEST_DIR\terraform.zip"
$EXE_PATH = "$DEST_DIR\terraform.exe"

# Create bin directory if it doesn't exist
if (-not (Test-Path $DEST_DIR)) {
    New-Item -ItemType Directory -Force -Path $DEST_DIR | Out-Null
}

# Download Terraform if not present
if (-not (Test-Path $EXE_PATH)) {
    Write-Host "Downloading Terraform $TF_VERSION..."
    Invoke-WebRequest -Uri $TF_URL -OutFile $ZIP_PATH
    
    Write-Host "Extracting Terraform..."
    Expand-Archive -Path $ZIP_PATH -DestinationPath $DEST_DIR -Force
    
    # Cleanup zip
    Remove-Item $ZIP_PATH -Force
    Write-Host "Terraform installed to $EXE_PATH"
} else {
    Write-Host "Terraform already exists at $EXE_PATH"
}

# Add to PATH for current session
$env:PATH = "$DEST_DIR;$env:PATH"
Write-Host "Added $DEST_DIR to PATH for this session."
Write-Host "You can now run 'terraform --version'"
