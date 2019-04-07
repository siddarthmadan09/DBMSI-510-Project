# First argument is the JDK path to be set. 
# All special characters should be escaped. eg. /usr/ => \/usr\/
if [ $# -eq 0 ]
  then
    var="JDKPATH = \$\(JAVA_HOME\)"
  else
    var="JDKPATH = $1"
fi
sed -i "1s/.*/$var/" ./catalog/Makefile
sed -i "1s/.*/$var/" ./btree/Makefile
sed -i "1s/.*/$var/" ./bufmgr/Makefile
sed -i "1s/.*/$var/" ./chainexception/Makefile
sed -i "1s/.*/$var/" ./global/Makefile
sed -i "1s/.*/$var/" ./heap/Makefile
sed -i "1s/.*/$var/" ./index/Makefile
sed -i "1s/.*/$var/" ./iterator/Makefile
sed -i "2s/.*/$var/" ./tests/Makefile
sed -i "9s/.*/$var/" ./Makefile
