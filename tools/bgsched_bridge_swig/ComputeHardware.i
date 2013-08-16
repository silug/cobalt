/*Autogenerated from python*/
%module pybgsched

%include <boost_shared_ptr.i>
/*%template(ConstPtr) boost::shared_ptr<const bgsched::ComputeHardware>;
%template(Ptr) boost::shared_ptr<bgsched::ComputeHardware>;
*/

//%shared_ptr(Ptr)
%shared_ptr(bgsched::ComputeHardware)

%template() bgsched::EnumWrapper<enum bgsched::Hardware::State>;

%{
#include <bgsched/ComputeHardware.h>
%}
%include "/bgsys/drivers/ppcfloor/hlcs/include/bgsched/ComputeHardware.h"

