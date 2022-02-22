import unicodedata

def clean_column_name(name):
	# For now, just remove control characters, ref: https://stackoverflow.com/a/19016117
	cleaned_name = "".join(ch for ch in name if unicodedata.category(ch)[0]!="C")
	return cleaned_name

# This might be more convenient, due to the way the results are parsed from metanome, but can be slow when used very often
def escape_control_characters(name):
	return name.encode("unicode_escape").decode("utf-8")
