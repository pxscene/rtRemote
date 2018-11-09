# rtRemote
rtRemote

Build Instructions for rtRemote (Linux only)

## Building with CMake

1. Build rtCore
   ~~~~
   git clone https://github.com/pxscene/pxCore
   cd pxCore
   mkdir temp
   cmake -DBUILD_RTCORE_LIBS=ON -DBUILD_PXCORE_LIBS=OFF -DBUILD_PXSCENE=OFF ..
   cmake --build .
   ~~~~

2. Build rtRemote
   ~~~~
   git clone https://github.com/pxscene/rtRemote.git
   cd rtRemote
   mkdir temp
   cd temp
   cmake -DRT_INCLUDE_DIR="path to pxCore/src/" -DRT_LIBRARY_DIR="path to pxCore/build/glut/" -DBUILD_RTREMOTE_LIBS=ON ..
   cmake --build . --config Release
   ~~~~

   The rtRemote libs will be located in rtRemote

   Additional build configurations for rtRemote are:
   ~~~~
   Build rpcSampleApp: -DBUILD_RTREMOTE_SAMPLE_APP_SHARED=ON
   Build rpcSampleApp_s: -DBUILD_RTREMOTE_SAMPLE_APP_STATIC=ON
   Build rtSampleClient and rtSampleServer: -DBUILD_RTREMOTE_SAMPLE_APP_SIMPLE=ON
   Enable rtRemote debugging: -DENABLE_RTREMOTE_DEBUG=ON
   Enable rtRemote profiling: -DENABLE_RTREMOTE_PROFILE=ON
   Disable librtremote shared library building: -DBUILD_RTREMOTE_SHARED_LIB=OFF
   Disable librtremote static library building: -DBUILD_RTREMOTE_STATIC_LIB=OFF
   ~~~~
