:: Build docker image
set BUILD_CMD=docker build -t decp_preprocessing .
echo Building docker image with command: %BUILD_CMD%
%BUILD_CMD%

:: Run docker image with mounted volume
call script/docker_run.bat
