package de.janhuddel.bpm.optimize.plugin;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import org.camunda.optimize.plugin.importing.variable.PluginVariableDto;
import org.camunda.optimize.plugin.importing.variable.VariableImportAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.flattener.JsonifyArrayList;

public class ComplexVariableFlattener
		implements VariableImportAdapter {

	private static final Logger logger = LoggerFactory.getLogger(ComplexVariableFlattener.class);

	// maximum length for variables of type string
	public static final int VALUE_MAX_LENGTH = 4000;

	// in the Camunda engine, it must be ensured that Date is serialized in this format.
	// (Engine Config (SPI): de.provinzial.bpm.spin.dataformat.JacksonDataFormatConfigurator)
	private static final DateFormat engineDateFormat = new StdDateFormat() //
			.withTimeZone(TimeZone.getDefault()) //
			.withColonInTimeZone(true);

	// ISO8601-formatted Date for Optimze (without colon)
	private static final DateFormat optimizeDateFormat = new StdDateFormat() //
			.withTimeZone(TimeZone.getDefault()) //
			.withColonInTimeZone(false);

	@Override
	public List<PluginVariableDto> adaptVariables(List<PluginVariableDto> variables) {
		List<PluginVariableDto> resultList = new ArrayList<>();
		for (PluginVariableDto pluginVariableDto : variables) {
			logger.debug("adapting variable {} (v{}) of process-instance {}...", //
					pluginVariableDto.getName(), //
					pluginVariableDto.getVersion(), //
					pluginVariableDto.getProcessInstanceId());
			if (pluginVariableDto.getType().equalsIgnoreCase("object")) {
				String serializationDataFormat = String.valueOf(pluginVariableDto.getValueInfo().get("serializationDataFormat"));
				if (serializationDataFormat.equals("application/json")) {
					this.flatJsonObject(pluginVariableDto, resultList);
				} else {
					logger.warn("complex variable '{}' won't be imported (unsupported serializationDataFormat: {})",
							pluginVariableDto.getName(), serializationDataFormat);
				}
			} else {
				resultList.add(pluginVariableDto);
			}
		}
		return resultList;
	}

	private void flatJsonObject(PluginVariableDto variable, List<PluginVariableDto> resultList) {
		if (variable.getValue() == null || variable.getValue().isEmpty()) {
			return;
		}

		try {
			new JsonFlattener(variable.getValue()) //
					.withFlattenMode(FlattenMode.KEEP_ARRAYS) //
					.flattenAsMap() //
					.entrySet() //
					.stream() //
					.map(e -> this.map(e.getKey(), e.getValue(), variable)) //
					.filter(Optional::isPresent) //
					.map(Optional::get) //
					.forEach(resultList::add);
		} catch (Throwable t) {
			logger.error("error while flattening variable '" + variable.getName() + "')", t);
		}
	}

	private Optional<PluginVariableDto> map(String name, Object value, PluginVariableDto origin) {
		// null-values are not supported
		if (value == null) {
			logger.info("variable-attribute '{}' of '{}' is null and won't be imported", name, origin.getName());
			return Optional.empty();
		}

		PluginVariableDto newVariable = new PluginVariableDto();

		// copy meta-info from origin
		newVariable.setEngineAlias(origin.getEngineAlias());
		newVariable.setProcessDefinitionId(origin.getProcessDefinitionId());
		newVariable.setProcessDefinitionKey(origin.getProcessDefinitionKey());
		newVariable.setProcessInstanceId(origin.getProcessInstanceId());
		newVariable.setVersion(origin.getVersion());

		// set name, type and value
		if ("root".equals(name)) {
			// the name "root" is used by the flattener if the JSON is a string or array (no object)
			newVariable.setName(origin.getName());
		} else {
			newVariable.setName(String.join(".", origin.getName(), name));
		}

		if (value instanceof JsonifyArrayList) {
			// for lists, only the number of contained elements is exported to reduce complexity
			newVariable.setName(String.join(".", newVariable.getName(), "_listsize"));
			newVariable.setType("Long");
			newVariable.setValue(String.valueOf(JsonifyArrayList.class.cast(value).size()));
		} else if (value instanceof String) {
			String stringValue = String.valueOf(value);

			Optional<Date> optDate = this.parseDate(stringValue);
			if (optDate.isPresent()) {
				newVariable.setType("Date");
				newVariable.setValue(optimizeDateFormat.format(optDate.get()));
			} else {
				newVariable.setType("String");
				if (stringValue.length() > VALUE_MAX_LENGTH) {
					logger.warn("value of variable {} will be truncated (original size: {})", newVariable.getName(), stringValue
							.length());
					stringValue = stringValue.substring(0, VALUE_MAX_LENGTH);
				}
				newVariable.setValue(stringValue);
			}
		} else if (value instanceof Boolean) {
			newVariable.setType("Boolean");
			newVariable.setValue(String.valueOf(value));
		} else if (value instanceof BigDecimal) {
			BigDecimal convertedValue = BigDecimal.class.cast(value);
			newVariable.setType("Double");
			newVariable.setValue(convertedValue.toString());
		} else {
			return notSupported(name, origin.getName(), value);
		}

		// When assigning the ID we have to consider that the importer calls the method adaptVariables(...) several times for one
		// variable version. If I always create a new unique ID at this point, a variable will be appended to the process instance
		// in Elastic several times (see discussion in: https://jira.camunda.com/browse/SUPPORT-7449)
		String newId = origin.getId() + "_" + newVariable.getName();
		newVariable.setId(newId);

		return Optional.of(newVariable);
	}

	private Optional<Date> parseDate(String dateAsString) {
		try {
			return Optional.of(engineDateFormat.parse(dateAsString));
		} catch (ParseException e) {
			return Optional.empty();
		}
	}

	private static Optional<PluginVariableDto> notSupported(String name, String originName, Object value) {
		logger.warn("variable-attribute '{}' of '{}' with type {} and value '{}' is not supported yet", name, originName, value
				.getClass()
				.getSimpleName(), String.valueOf(value));
		return Optional.empty();
	}
}
