to deploy project in Dropbox follow this guide: http://www.jitblog.net/use-dropbox-to-host-a-maven-repository/

main command: mvn deploy -DskipTests=true -DaltDeploymentRepository=dropbox::default::file:///C:/Dropbox/Public/REPOSITORY  
