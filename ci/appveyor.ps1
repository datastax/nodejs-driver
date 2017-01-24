$env:JAVA_HOME="C:\Program Files\Java\jdk1.8.0"
$env:PYTHON="C:\Python27-x64"
$env:PATH="$($env:PYTHON);$($env:PYTHON)\Scripts;$($env:JAVA_HOME)\bin;$($env:PATH)"
$env:CCM_PATH="C:\Users\appveyor\ccm"

$dep_dir="C:\Users\appveyor\deps"
If (!(Test-Path $dep_dir)) {
  Write-Host "Creating $($dep_dir)"
  New-Item -Path $dep_dir -ItemType Directory -Force
}

# Install Java Cryptographic Extensions, needed for SSL.
$target = "$($env:JAVA_HOME)\jre\lib\security"
# If this file doesn't exist we know JCE hasn't been installed.
$jce_indicator = "$target\README.txt"
$zip = "$($dep_dir)\jce_policy-8.zip"
If (!(Test-Path $jce_indicator)) {
  Write-Host "Installing JCE for jdk8"
  # Download zip to staging area if it doesn't exist, we do this because
  # we extract it to the directory based on the platform and we want to cache
  # this file so it can apply to all platforms.
  if(!(Test-Path $zip)) {
    $url = "https://www.dropbox.com/s/al1e6e92cjdv7m7/jce_policy-8.zip?dl=1"
    (new-object System.Net.WebClient).DownloadFile($url, $zip)
  }

  Add-Type -AssemblyName System.IO.Compression.FileSystem
  [System.IO.Compression.ZipFile]::ExtractToDirectory($zip, $target)

  $jcePolicyDir = "$target\UnlimitedJCEPolicyJDK8"
  Move-Item $jcePolicyDir\* $target\ -force
  Remove-Item $jcePolicyDir
}

# Install Ant
$ant_base = "$($dep_dir)\ant"
$ant_path = "$($ant_base)\apache-ant-1.9.7"
If (!(Test-Path $ant_path)) {
  Write-Host "Installing Ant"
  $ant_url = "https://www.dropbox.com/s/lgx95x1jr6s787l/apache-ant-1.9.7-bin.zip?dl=1"
  $ant_zip = "C:\Users\appveyor\apache-ant-1.9.7-bin.zip"
  (new-object System.Net.WebClient).DownloadFile($ant_url, $ant_zip)
  [System.IO.Compression.ZipFile]::ExtractToDirectory($ant_zip, $ant_base)
}
$env:PATH="$($ant_path)\bin;$($env:PATH)"

$env:CCM_PATH="$($dep_dir)\ccm"
# Clone CCM from git, it will only be used to access keystores for SSL.
If (!(Test-Path $env:CCM_PATH)) {
  Start-Process git -ArgumentList "clone https://github.com/pcmanus/ccm.git $($env:CCM_PATH)" -Wait -NoNewWindow
}

# Install Python Dependencies for CCM.
Write-Host "Installing CCM and its dependencies"
Start-Process python -ArgumentList "-m pip install psutil pyYaml six ccm" -Wait -NoNewWindow

# Predownload cassandra version for CCM if it isn't already downloaded.
If (!(Test-Path C:\Users\appveyor\.ccm\repository\$env:TEST_CASSANDRA_VERSION)) {
  Write-Host "Predownloading $env:TEST_CASSANDRA_VERSION"
  Start-Process python -ArgumentList "$env:PYTHON\Scripts\ccm.py create -v $($env:TEST_CASSANDRA_VERSION) -n 1 predownload" -Wait -NoNewWindow
  Start-Process python -ArgumentList "$env:PYTHON\Scripts\ccm.py remove predownload" -Wait -NoNewWindow
} else {
  Write-Host "Cassandra $env:TEST_CASSANDRA_VERSION was already preloaded"
}

# Activate the proper nodejs version.
Install-Product node $env:nodejs_version $env:PLATFORM

# Enable appveyor reporter
$env:multi="spec=- mocha-appveyor-reporter=-"
