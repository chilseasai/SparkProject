/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package keyword.indexation.datatype;

import lombok.Data;

import java.io.Serializable;

/**
 * KeywordIndexationData
 *
 * @author cn-seo-dev@
 */
@Data
public class KeywordIndexationData implements Serializable {

    private Long marketplace_id;

    private String page_id;

    private String keywords;

    private Long transits;

    private Double ops;

    private Integer index;

    private String index_reason;

    private String trigger_keyword;
}
