$ips = Get-Content -Path "C:\Path\To\IPList.txt" # Replace with the path to your IP list file
$outputFile = "ping-results.csv" # Replace with your desired output file name and path

$results = @()
foreach ($ip in $ips) {
    $ping = Test-Connection -ComputerName $ip -Count 1 -Quiet
    if ($ping) {
        $status = "Online"
    } else {
        $status = "Offline"
    }
    $results += [PSCustomObject]@{
        IP = $ip
        Status = $status
    }
}

$results | Export-Csv -Path $outputFile -NoTypeInformation




Sub SplitEachWorksheet()
Dim FPath As String
FPath = Application.ActiveWorkbook.Path
Application.ScreenUpdating = False
Application.DisplayAlerts = False
For Each ws In ThisWorkbook.Sheets
    ws.Copy
    Application.ActiveWorkbook.SaveAs Filename:=FPath & "\" & ws.Name & ".xlsx"
    Application.ActiveWorkbook.Close False
Next
Application.DisplayAlerts = True
Application.ScreenUpdating = True
End Sub