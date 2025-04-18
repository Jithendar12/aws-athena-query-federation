/*-
 * #%L
 * athena-aws-cmdb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.aws.cmdb.tables.ec2;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.BlockDeviceMapping;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeImagesResponse;
import software.amazon.awssdk.services.ec2.model.EbsBlockDevice;
import software.amazon.awssdk.services.ec2.model.Image;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.util.Collections;
import java.util.List;

/**
 * Maps your EC2 images (aka AMIs) to a table.
 */
public class ImagesTableProvider
        implements TableProvider
{
    private static final String DEFAULT_OWNER_ENV = "default_ec2_image_owner";
    private static final int MAX_IMAGES = 1000;
    //Sets a default owner filter (when not null) to reduce the number of irrelevant AMIs returned when you do not
    //query for a specific owner.
    private final String defaultOwner;
    private static final Schema SCHEMA;
    private Ec2Client ec2;

    public ImagesTableProvider(Ec2Client ec2, java.util.Map<String, String> configOptions)
    {
        this.ec2 = ec2;
        this.defaultOwner = configOptions.get(DEFAULT_OWNER_ENV);
    }

    /**
     * @See TableProvider
     */
    @Override
    public String getSchema()
    {
        return "ec2";
    }

    /**
     * @See TableProvider
     */
    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "ec2_images");
    }

    /**
     * @See TableProvider
     */
    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SCHEMA);
    }

    /**
     * Calls DescribeImagess on the AWS EC2 Client returning all images that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific volumes) to EC2.
     *
     * @note Because of the large number of public AMIs we also support using a default 'owner' filter if your query doesn't
     * filter on owner itself. You can set this using an env variable on your Lambda function defined by DEFAULT_OWNER_ENV.
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        DescribeImagesRequest.Builder request = DescribeImagesRequest.builder();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("id");
        ValueSet ownerConstraint = recordsRequest.getConstraints().getSummary().get("owner");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.imageIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }
        else if (ownerConstraint != null && ownerConstraint.isSingleValue()) {
            request.owners(Collections.singletonList(ownerConstraint.getSingleValue().toString()));
        }
        else if (defaultOwner != null) {
            request.owners(Collections.singletonList(defaultOwner));
        }
        else {
            throw new RuntimeException("A default owner account must be set or the query must have owner" +
                    "in the where clause with exactly 1 value otherwise results may be too big.");
        }

        DescribeImagesResponse response = ec2.describeImages(request.build());

        int count = 0;
        for (Image next : response.images()) {
            if (count++ > MAX_IMAGES) {
                throw new RuntimeException("Too many images returned, add an owner or id filter.");
            }
            instanceToRow(next, spiller);
        }
    }

    /**
     * Maps an EC2 Image (AMI) into a row in our Apache Arrow response block(s).
     *
     * @param image The EC2 Image (AMI) to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(Image image,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("id", row, image.imageId());
            matched &= block.offerValue("architecture", row, image.architectureAsString());
            matched &= block.offerValue("created", row, image.creationDate());
            matched &= block.offerValue("description", row, image.description());
            matched &= block.offerValue("hypervisor", row, image.hypervisorAsString());
            matched &= block.offerValue("location", row, image.imageLocation());
            matched &= block.offerValue("type", row, image.imageTypeAsString());
            matched &= block.offerValue("kernel", row, image.kernelId());
            matched &= block.offerValue("name", row, image.name());
            matched &= block.offerValue("owner", row, image.ownerId());
            matched &= block.offerValue("platform", row, image.platformAsString());
            matched &= block.offerValue("ramdisk", row, image.ramdiskId());
            matched &= block.offerValue("root_device", row, image.rootDeviceName());
            matched &= block.offerValue("root_type", row, image.rootDeviceTypeAsString());
            matched &= block.offerValue("srvio_net", row, image.sriovNetSupport());
            matched &= block.offerValue("state", row, image.stateAsString());
            matched &= block.offerValue("virt_type", row, image.virtualizationTypeAsString());
            matched &= block.offerValue("is_public", row, image.publicLaunchPermissions());

            List<Tag> tags = image.tags();
            matched &= block.offerComplexValue("tags",
                    row,
                    (Field field, Object val) -> {
                        if (field.getName().equals("key")) {
                            return ((Tag) val).key();
                        }
                        else if (field.getName().equals("value")) {
                            return ((Tag) val).value();
                        }

                        throw new RuntimeException("Unexpected field " + field.getName());
                    },
                    tags);

            matched &= block.offerComplexValue("block_devices",
                    row,
                    (Field field, Object val) -> {
                        if (field.getName().equals("dev_name")) {
                            return ((BlockDeviceMapping) val).deviceName();
                        }
                        else if (field.getName().equals("no_device")) {
                            return ((BlockDeviceMapping) val).noDevice();
                        }
                        else if (field.getName().equals("virt_name")) {
                            return ((BlockDeviceMapping) val).virtualName();
                        }
                        else if (field.getName().equals("ebs")) {
                            return ((BlockDeviceMapping) val).ebs();
                        }
                        else if (field.getName().equals("ebs_size")) {
                            return ((EbsBlockDevice) val).volumeSize();
                        }
                        else if (field.getName().equals("ebs_iops")) {
                            return ((EbsBlockDevice) val).iops();
                        }
                        else if (field.getName().equals("ebs_type")) {
                            return ((EbsBlockDevice) val).volumeTypeAsString();
                        }
                        else if (field.getName().equals("ebs_kms_key")) {
                            return ((EbsBlockDevice) val).kmsKeyId();
                        }

                        throw new RuntimeException("Unexpected field " + field.getName());
                    },
                    image.blockDeviceMappings());

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("id")
                .addStringField("architecture")
                .addStringField("created")
                .addStringField("description")
                .addStringField("hypervisor")
                .addStringField("location")
                .addStringField("type")
                .addStringField("kernel")
                .addStringField("name")
                .addStringField("owner")
                .addStringField("platform")
                .addStringField("ramdisk")
                .addStringField("root_device")
                .addStringField("root_type")
                .addStringField("srvio_net")
                .addStringField("state")
                .addStringField("virt_type")
                .addBitField("is_public")
                .addField(
                        FieldBuilder.newBuilder("tags", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("tag", Types.MinorType.STRUCT.getType())
                                                .addStringField("key")
                                                .addStringField("value")
                                                .build())
                                .build())
                .addField(
                        FieldBuilder.newBuilder("block_devices", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("device", Types.MinorType.STRUCT.getType())
                                                .addStringField("dev_name")
                                                .addStringField("no_device")
                                                .addStringField("virt_name")
                                                .addField(
                                                        FieldBuilder.newBuilder("ebs", Types.MinorType.STRUCT.getType())
                                                                .addIntField("ebs_size")
                                                                .addIntField("ebs_iops")
                                                                .addStringField("ebs_type")
                                                                .addStringField("ebs_kms_key")
                                                                .build())
                                                .build())
                                .build())
                .addMetadata("id", "The id of the image.")
                .addMetadata("architecture", "The architecture required to run the image.")
                .addMetadata("created", "The date and time the image was created.")
                .addMetadata("description", "The description associated with the image.")
                .addMetadata("hypervisor", "The type of hypervisor required by the image.")
                .addMetadata("location", "The location of the image.")
                .addMetadata("type", "The type of image.")
                .addMetadata("kernel", "The kernel used by the image.")
                .addMetadata("name", "The name of the image.")
                .addMetadata("owner", "The owner of the image.")
                .addMetadata("platform", "The platform required by the image.")
                .addMetadata("ramdisk", "Detailed of the ram disk used by the image.")
                .addMetadata("root_device", "The root device used by the image.")
                .addMetadata("root_type", "The type of root device required by the image.")
                .addMetadata("srvio_net", "Details of srvio network support in the image.")
                .addMetadata("state", "The state of the image.")
                .addMetadata("virt_type", "The type of virtualization supported by the image.")
                .addMetadata("is_public", "True if the image is publically available.")
                .addMetadata("tags", "Tags associated with the image.")
                .addMetadata("block_devices", "Block devices required by the image.")
                .build();
    }
}
