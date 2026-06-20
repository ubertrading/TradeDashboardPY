1. .NET 8.0 Runtime
Since the service is a .NET 8 Web API project (net8.0), you must install the ASP.NET Core Runtime 8.0 (x64) or .NET SDK 8.0 (x64) on the machine.

Download link: dotnet.microsoft.com/download/dotnet/8.0
Select the ASP.NET Core Runtime 8.0 under the "Run apps" column for Windows.
2. Python Dependencies (for the dashboard)
Ensure you have the required Python libraries installed:

powershell
pip install Flask python-dateutil tzdata requests pythonnet


-------------------
https://github.com/copilot/share/8a7142aa-48e4-80b1-9911-8e46a4672888

download/install python-3.12.9-amd64.exe

Download and install .NET Runtime 8.0+ from: https://dotnet.microsoft.com/download

minimal requirements:
put this in file and run
pip install -r _requirements.txt

Flask==3.1.3
python-dateutil==2.9.0.post0
tzdata==2025.3
requests==2.32.3
pythonnet==3.0.5


