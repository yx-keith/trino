/*
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
package io.trino.sql.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJoinUsing
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testColumnReferences()
    {
        assertThat(assertions.query(
                "SELECT k, v1, v2, t.v1, u.v2 FROM " +
                        "(VALUES (1, 'a')) AS t(k, v1) JOIN" +
                        "(VALUES (1, 'b')) AS u(k, v2) USING (k)"))
                .matches("VALUES (1, 'a', 'b', 'a', 'b')");

        assertThatThrownBy(() -> assertions.query(
                "SELECT t.k FROM " +
                        "(VALUES (1, 'a')) AS t(k, v1) JOIN" +
                        "(VALUES (1, 'b')) AS u(k, v2) USING (k)"))
                .hasMessageMatching(".*Column 't.k' cannot be resolved.*");
    }

    @Test
    public void testInner()
    {
        assertThat(assertions.query(
                "SELECT * FROM " +
                        "(VALUES (1, 'a')) AS t(k, v1) JOIN" +
                        "(VALUES (1, 'b')) AS u(k, v2) USING (k)"))
                .matches("VALUES (1, 'a', 'b')");
    }

    @Test
    public void testMultipleKeys()
    {
        assertThat(assertions.query(
                "SELECT * FROM " +
                        "(VALUES (1, 'a', 2)) AS t(k1, v1, k2) JOIN" +
                        "(VALUES (1, 'b', 2)) AS u(k1, v2, k2) USING (k1, k2)"))
                .matches("VALUES (1, 2, 'a', 'b')");
    }

    @Test
    public void testCoercion()
    {
        assertThat(assertions.query(
                "SELECT * FROM " +
                        "(VALUES (1e0, 'a')) AS t(k, v1) JOIN" +
                        "(VALUES (1, 'b')) AS u(k, v2) USING (k)"))
                .matches("VALUES (1e0, 'a', 'b')");

        // long, double
        assertThat(assertions.query(
                "SELECT * FROM (VALUES (1, 2e0)) x (a, b) JOIN (VALUES (DOUBLE '1.0', 3)) y (a, b) USING(a)"))
                .matches("VALUES (1e0, 2e0, 3)");

        // double, long
        assertThat(assertions.query(
                "SELECT * FROM (VALUES (1.0E0, 2e0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a)"))
                .matches("VALUES (1e0, 2e0, 3)");

        // long decimal, bigint
        assertThat(assertions.query(
                "SELECT * FROM (VALUES (DECIMAL '0000000000000000001', 2e0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a)"))
                .matches("VALUES (CAST(1 AS DECIMAL(10, 0)), 2e0, 3)");

        // bigint, long decimal
        assertThat(assertions.query(
                "SELECT * FROM (VALUES (1, 2e0)) x (a, b) JOIN (VALUES (DECIMAL '0000000000000000001', 3)) y (a, b) USING(a)"))
                .matches("VALUES (CAST(1 AS DECIMAL(10, 0)), 2e0, 3)");

        // bigint, short decimal
        assertThat(assertions.query(
                "SELECT * FROM (VALUES (1, 2e0)) x (a, b) JOIN (VALUES (1e0, 3)) y (a, b) USING(a)"))
                .matches("VALUES (1e0, 2e0, 3)");

        // short decimal, bigint
        assertThat(assertions.query(
                "SELECT * FROM (VALUES (1e0, 2e0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a)"))
                .matches("VALUES (1e0, 2e0, 3)");
        assertThat(assertions.query(
                "SELECT * FROM (VALUES (1, 2)) x (a, b) JOIN (VALUES (CAST (1 AS SMALLINT), CAST(3 AS SMALLINT))) y (a, b) USING(a)"))
                .matches("VALUES (1, 2, SMALLINT '3')");
    }

    @Test
    public void testDuplicateColumns()
    {
        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM " +
                        "(VALUES (1, 'a')) AS t(k, v1) JOIN" +
                        "(VALUES (1, 'b')) AS u(k, v2) USING (k, k)"))
                .hasMessageMatching(".*Column 'k' appears multiple times in USING clause.*");
    }

    @Test
    public void testAlternateColumnOrders()
    {
        assertThat(assertions.query(
                "SELECT * FROM " +
                        "(VALUES ('a', 1)) AS t(v1, k) JOIN" +
                        "(VALUES (1, 'b')) AS u(k, v2) USING (k)"))
                .matches("VALUES (1, 'a', 'b')");
    }

    @Test
    public void testOuter()
    {
        assertThat(assertions.query(
                "SELECT * FROM " +
                        "(VALUES (1, 'a')) AS t(k, v1) LEFT JOIN" +
                        "(VALUES (2, 'b')) AS u(k, v2) USING (k)"))
                .matches("VALUES (1, 'a', CAST(NULL AS VARCHAR(1)))");

        assertThat(assertions.query(
                "SELECT * FROM " +
                        "(VALUES (1, 'a')) AS t(k, v1) RIGHT JOIN" +
                        "(VALUES (2, 'b')) AS u(k, v2) USING (k)"))
                .matches("VALUES (2, CAST(NULL AS VARCHAR(1)), 'b')");

        assertThat(assertions.query(
                "SELECT * FROM " +
                        "(VALUES (0, 1, 2), (3, 6, 5)) a(x1, y, z1) FULL OUTER JOIN " +
                        "(VALUES (3, 1, 5), (0, 4, 2)) b(x2, y, z2) USING (y)"))
                .matches("VALUES " +
                        "(1, 0, 2, 3, 5), " +
                        "(6, 3, 5, CAST(NULL AS INTEGER), CAST(NULL AS INTEGER)), " +
                        "(4, CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), 0, 2)");

        assertThat(assertions.query(
                "SELECT y, x1, x2 FROM " +
                        "(VALUES (0, 1, 2), (3, 6, 5), (3, null, 5)) a(x1, y, z1) FULL OUTER JOIN " +
                        "(VALUES (3, 1, 5), (0, 4, 2)) b(x2, y, z2) USING (y)"))
                .matches("VALUES " +
                        "(1, 0, 3), " +
                        "(6, 3, CAST(NULL AS INTEGER)), " +
                        "(CAST(NULL AS INTEGER), 3, CAST(NULL AS INTEGER)), " +
                        "(4, CAST(NULL AS INTEGER), 0)");
    }

    @Test
    public void testDuplicateAliases()
    {
        assertThat(assertions.query(
                "WITH t(k, v) AS (VALUES (1, 'a'))" +
                        "SELECT * FROM t JOIN t USING (k)"))
                .matches("VALUES (1, 'a', 'a')");
    }

    @Test
    public void testDecimal()
    {
        assertThat(assertions.query(
                "SELECT * FROM (VALUES (1.0, 2.0)) x (a, b) JOIN (VALUES (1.0, 3.0)) y (a, b) USING(a)"))
                .matches("VALUES (1.0, 2.0, 3.0)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES (123456789123456789.123456, 2.0)) x (a, b) JOIN (VALUES (123456789123456789.123456, 3.0)) y (a, b) USING(a)"))
                .matches("VALUES (123456789123456789.123456, 2.0, 3.0)");
    }
}
