function cleanDate(value) {
	var today = new Date();
	if (value > today.getFullYear() + 1 || value < 1000) {
		return "1970";
	}
	return value;
}
