Copyright (c) 2006-, IPD Boehm, Universitaet Karlsruhe (TH) / KIT, by Guido Sautter
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the Universität Karlsruhe (TH) nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY UNIVERSITÄT KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


This project is provides the GoldenGATE Server Core, components for user
management and remote event handling, GoldenGATE Server Web Front-end core,
modules for online user management, a GoldenGATE Editor plugin for centralized
authentication against a GoldenGATE Server, as well as many unitilty and
convenience classes for building server components and web front-end servlets
and modules.


This project requires the JAR files build by the Ant script in the idaho-core
project (http://code.google.com/p/idaho-core/) and the JAR files referenced from
there. See http://code.google.com/p/idaho-core/source/browse/README.txt for the
latter. You can either check out idaho-core as a project into the same workspace
and build it first, or include the JAR files it generates in the "lib" folder.

The GoldenGATE Editor plug-ins further depend on the goldengate-editor project
(http://code.google.com/p/goldengate-editor/) and the JAR files referenced from
there. See http://code.google.com/p/goldengate-editor/source/browse/README.txt
for the latter. If neither of the goldengate-editor project and the GoldenGATE.jar
it builds into are avaliable, the build as a whole won't fail, only the GoldenGATE
Editor plug-ins won't be created.