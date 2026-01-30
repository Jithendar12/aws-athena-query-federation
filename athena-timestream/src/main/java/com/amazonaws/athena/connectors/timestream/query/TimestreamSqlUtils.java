/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.timestream.query;

import org.stringtemplate.v4.ST;

import java.util.Map;

/**
 * Utilities for rendering StringTemplate templates in Timestream connector.
 */
public class TimestreamSqlUtils
{
    private TimestreamSqlUtils() {}

    /**
     * Renders a StringTemplate with the given parameters.
     *
     * @param queryFactory The factory to get the template from.
     * @param templateName The name of the template to render.
     * @param params Map of parameter key-value pairs.
     * @return The rendered template string.
     */
    public static String renderTemplate(QueryFactory queryFactory, String templateName, Map<String, Object> params)
    {
        ST template = queryFactory.getQueryTemplate(templateName);
        if (template == null) {
            throw new RuntimeException("Template not found: " + templateName);
        }

        // Add all parameters from the map
        params.forEach(template::add);

        return template.render().trim();
    }
}
