/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.debezium;

import io.debezium.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/** used for print connector configuration masked password which copy from debezium*/
public interface ConfigurationPrinter {
    Logger LOG = LoggerFactory.getLogger(ConfigurationPrinter.class);

    Pattern PASSWORD_PATTERN =
            Pattern.compile(
                    ".*password$|.*sasl\\.jaas\\.config$|.*basic\\.auth\\.user\\.info|.*registry\\.auth\\.client-secret",
                    Pattern.CASE_INSENSITIVE);

    default Configuration withMaskedPasswords(Properties properties) {
        return withMasked(PASSWORD_PATTERN, properties);
    }

    default Configuration withMaskedPasswords(Map<String, String> configuration) {
        Properties properties = new Properties();
        configuration
                .entrySet()
                .forEach((entry) -> properties.put(entry.getKey(), entry.getValue()));
        return withMasked(PASSWORD_PATTERN, properties);
    }

    /**
     * Return a new {@link Configuration} that contains all of the same fields as this
     * configuration, except with masked values for all keys that match the specified pattern.
     *
     * @param keyRegex the regular expression to match against the keys
     * @return the Configuration with masked values for matching keys; never null
     */
    default Configuration withMasked(Pattern keyRegex, Properties properties) {
        if (keyRegex == null) {
            return Configuration.from(properties);
        }
        return new Configuration() {
            @Override
            public Set<String> keys() {
                return Configuration.from(properties).keys();
            }

            @Override
            public String getString(String key) {
                boolean matches = keyRegex.matcher(key).matches();
                return matches ? "********" : Configuration.from(properties).getString(key);
            }

            @Override
            public String toString() {
                return withMaskedPasswords().asProperties().toString();
            }
        };
    }

    default void printConfigurationMaskedPasswords(Properties properties){
        LOG.info("Starting {} with configuration:", getClass().getSimpleName());
        withMaskedPasswords(properties)
            .asProperties()
            .forEach(
                (propName, propValue) -> {
                    LOG.info("   {} = {}", propName, propValue);
                });
    }
}
