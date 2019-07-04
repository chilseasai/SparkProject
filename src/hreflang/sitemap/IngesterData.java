/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package hreflang.sitemap;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;

/**
 * IngesterData
 *
 * @author cn-seo-dev@
 */
@Data
@AllArgsConstructor
public class IngesterData {

    private String id;
    private String href;
    private String hreflang;
    private Timestamp last_update_time;
    private Integer is_del;
    private Integer partition_id;
}
