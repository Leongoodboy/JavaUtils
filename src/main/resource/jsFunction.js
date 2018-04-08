function contains(value,data,operation){
	var result=true;
	if(operation.toLowerCase()=='and'){ 
		for (var i in data){
			if(value.indexOf(data[i])==-1){
				result=false;
				break;
			}

		}   
		
	}else if(operation.toLowerCase()=='or'){
		result=false;  
		for (var i in data){
			if(value.indexOf(data[i])>-1){
				result=true;
				break;
			}

		}   
	}else{
		result=false;
	}
	return result;
}
function notContains(value,data,operation){
	var result=true;
	if(operation.toLowerCase()=='and'){
		for (var i in data){
			if(value.indexOf(data[i])>-1){
				result=false;
				break;
			}

		}   
		
	}else if(operation.toLowerCase()=='or'){
		result=false;
		for (var i in data){
			if(value.indexOf(data[i])==-1){
				result=true;
				break;
			}

		}   
	}else{
		result=false;
	}
	return result;
}