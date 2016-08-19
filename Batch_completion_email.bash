#!/usr/bin/ksh
#
#
#########################################################################
#
# Script:  Batch_completion_email.ksh
#
# Version: 1.2
#
# Purpose: Send customised emails to pre-identified recipients based 
#          On parameterised batch code passed in. 
#
# Usage:   Batch_completion_email.ksh -S <System Codes> (from XDSL,LOLO, 
#          MOBILES, WMEMSN , Bigdata or PERU)
#
# Parameters:
#          S            System CODE XDSL,LOLO, MOBILES,CSS or PERU
#
#########################################################################
# Ver. | Name       	| Date       | Modification
#-------------------------------------------------------------------
# 1.0  | N. Peiris  	| 21-08-2007 | Initial Coding
# 1.1  | G.Pavan    	| 01-06-2009 | Modified Message Body for INC1109045 
# 1.2  | Irfan Khan 	| 24-05-2010 | Included HP team email id
# 1.3  | Rajil      	| 07-04-2011 | Changed Mob. no. for SMS notification.Ref:INC000004272588
# 1.4  | Rajil      	| 24-08-2011 | Disable notification to users Ref:CRQ000000592131
# 1.5  | Jeevanantham C | 27-03-2015 | Added New interface WMEMSN batch completion email
#########################################################################

 
######################################################################
# Set-Up variables passed into script.
####################################################################### 
#
# Initialise variables that are optional command
# line options.
#
#

# clear parameters
       
unset _From 
unset _To 
unset _Cc 
unset _Cc 
unset _Bcc 
unset _Subject 
unset _EmailBody
unset _System_Code


Usage="${0}
       [-S]     System Codes (XDSL,LOLO, MOBILES, WMEMSN,CSS or PERU)"
                
# Temp variables.
_Job_Name=Batch_completion_email.ksh


if [[ $# != 0 ]]
then
        # Process Command Options
        while getopts S: OPTIONS
        do
             case $OPTIONS in
                S)              _System_Code=$OPTARG;;
                \?)     echo "ERROR: Option ${OPTARG} is invalid..."
                        echo "USAGE: ${Usage}"
                    exit 2;;
          esac
        done

else
        echo "ERROR: No options given."
        echo "USAGE: ${Usage}"
        exit 2 
fi


if [[ 
       "$_System_Code" = ""    
   ]]
then
echo "ERROR: No values provided for -S. Required."
echo "USAGE: ${Usage}"
exit 2
fi



###########
# Main Logic
##

# Use only one SMPT for the "_From" variable
# When using multiple SMPT for  _To , _Cc  and  _Bcc 
# separate by "," without adding any space 
# Ex :  _Cc = personABC@ABC.com,personCDE@ABC.com

_From="sharathkumark@flipkart.com"

# Customise your email message body here
# When you need customised based on the system code. Add this variable 
# Inside the case statement

_EmailBody="
* This is an automated notification *

Should you have further queries

- For WDR IT Support matters, please contact WDR PS&M Manager on (03) 8866 7899
- For WDR Business Support matters, please contact \"! WDR Business Support\"  or  SMTP: F0402070@team.telstra.com

Kind regards,
WDR IT Support
(03) 8833 6490"

# Enable this line if you wish to use blind carbon copy option
# _Bcc="F0501885@team.telstra.com"
    
case $_System_Code in
  XDSL)         
    _Subject="WDR XDSL Daily is available"
    _To="edswdrpsm@hpe.com"
    _Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;

  XDSLW)
    _Subject="WDR XDSL Weekly Saturday is available"
    _To="edswdrpsm@hpe.com"
    _Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;

  XDSLS)
    _Subject="WDR XDSL Weekly Sunday is available"
    _To="edswdrpsm@hpe.com"
    _Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;

  XDSLM)
    _Subject="WDR XDSL Monthly is available"
    _To="edswdrpsm@hpe.com"
    _Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;
                
  LOLO)         
    _Subject="WDR LOLO Daily is available"
    _To="edswdrpsm@hpe.com"
    _Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;

  MOBILES)              
    _Subject="WDR Mobiles Daily is available"
    _To="edswdrpsm@hpe.com"
    _Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;

  PERU)         
    _Subject="WDR PERU Daily is available"
    _To="edswdrpsm@hpe.com"
    _Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;

  WMEMSN)
    _Subject="File Transfer of WME-MSN-DSF feed completed sucessfully"
    _To="edswdrpsm@hpe.com"
    _Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;

  BDT)
    _Subject="File Transfer of WDR-BDT-DSF feed completed sucessfully"
    _To="sharathkumark@flipkart.com"
    _Cc="sharathkumark@flipkart.com"
	_Bcc="sharathkumark@flipkart.com"
    ;;

 CSS)
    _Subject="Daily Cloud Services is available"
    _To="edswdrpsm@hpe.com"
    #_Cc="0400060498@SMS.IN.TELSTRA.COM.AU"
    ;;   
   *)     echo "System code is invalid..."
   echo "select one from XDSL, LOLO, MOBILES, WMEMSN or PERU"
   exit 2;;
esac
    
#  Enable this command to use Bcc. 
#  mailx -s "${_Subject}" -c ${_Cc} -r ${_From} -b ${_Bcc} ${_To} <<-EOF
#  ${_EmailBody}

   mailx -s "${_Subject}" -r "${_From}" -c "${_Cc}" "${_To}" <<-EOF${_EmailBody}


