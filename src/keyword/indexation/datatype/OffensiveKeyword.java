/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package keyword.indexation.datatype;

import lombok.Data;

import java.io.Serializable;

/**
 * The POJO which is offensive/copyrighted
 * data type.
 *
 * @author cn-seo-dev@
 */
@Data
public class OffensiveKeyword implements Serializable {

    private String trigger_keyword;

    private String keyword_type;

    private String marketplace_ids;

    private String date_added;

    private Double ops_loss;
}
