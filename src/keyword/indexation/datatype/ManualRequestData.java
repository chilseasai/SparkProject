/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package keyword.indexation.datatype;

import lombok.Data;

/**
 * ManualRequestData
 *
 * @author cn-seo-dev@
 */
@Data
public class ManualRequestData {

    private Integer marketplace_id;

    private String page_id;

    private String keywords;

    private Integer transits;

    private Double ops;

    private Integer index;

    private String index_reason;
}
