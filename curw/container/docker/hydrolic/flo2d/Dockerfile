#FROM microsoft/windowsservercore
FROM microsoft/dotnet-framework

RUN ["powershell", "New-Item", "-ItemType", "Directory","c:\\flo2d"]

# ADD  ["https://www.flo-2d.com/wp-content/uploads/2012/08/FLO-2D-Pro-Model.zip", "c:/flo2d/flo2d.zip" ]

ADD ["FLO-2D-Pro-Model.zip", "/flo2d/" ]

COPY ["Activator FLO-2D March Renewal.exe", "/flo2d/"]

WORKDIR c:\flo2d

RUN ["powershell", "Expand-Archive", "c:\\flo2d\\FLO-2D-Pro-Model.zip", "-DestinationPath", "c:\\flo2d\\FLO-2D-Pro-Model"]

RUN ["powershell", "New-Item", "-ItemType", "Directory","c:\\flo2d\\flo2d_model"]

RUN ["powershell", "New-Item", "-ItemType", "Directory","c:\\flo2d\\input"]

COPY ["FLO-2D PRO  from dropbox 2-15-2018", "/flo2d/flo2d_model"]