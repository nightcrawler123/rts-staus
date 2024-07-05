# PowerShell script to download a KB update from the Microsoft Update Catalog
param (
    [string]$KBNumber
)

$SearchURL = "https://www.catalog.update.microsoft.com/Search.aspx?q=$KBNumber"

# Create a session to bypass security and execute JavaScript
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
$response = Invoke-WebRequest -Uri $SearchURL -WebSession $session

# Parse the HTML to find the download link
$matches = [regex]::Matches($response.Content, "window.open\('(.*?)'")
$popupURL = $matches[0].Groups[1].Value

$popupResponse = Invoke-WebRequest -Uri $popupURL -WebSession $session
$downloadLink = [regex]::Match($popupResponse.Content, "href=""(http.*?.msu)""").Groups[1].Value

# Download the update
$OutputPath = "$PWD\$KBNumber.msu"
Invoke-WebRequest -Uri $downloadLink -OutFile $OutputPath

Write-Output "Downloaded $KBNumber to $OutputPath"
