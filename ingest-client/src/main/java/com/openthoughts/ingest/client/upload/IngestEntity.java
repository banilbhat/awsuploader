package com.openthoughts.ingest.client.upload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

public class IngestEntity {
  private String assetId;
  private String fileName;
  private String sourceFolderPath;

  @JsonCreator
  public IngestEntity(@JsonProperty(ASSET_ID) final String assetId,
      @JsonProperty(SOURCE_FOLDER_PATH) final String sourcefolderPath, @JsonProperty(FILE_NAME) final String fileName) {
    this.assetId = assetId;
    this.sourceFolderPath = sourcefolderPath;
    this.fileName = fileName;
  }

  @JsonProperty(SOURCE_FOLDER_PATH)
  public String getSourceFolderPath() {
    return sourceFolderPath;
  }

  @JsonProperty(ASSET_ID)
  public String getAssetId() {
    return assetId;
  }

  @JsonProperty(FILE_NAME)
  public String getFileName() {
    return fileName;
  }

  public void setFileName(final String fileName) {
    this.fileName = fileName;
  }

  public void setSourceFolderPath(final String sourceFolderPath) {
    this.sourceFolderPath = sourceFolderPath;
  }

  public void setAssetId(final String assetId) {
    this.assetId = assetId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(IngestEntity.class).add(ASSET_ID, getAssetId())
        .add(SOURCE_FOLDER_PATH, getSourceFolderPath()).add(FILE_NAME, getFileName()).toString();
  }

  private static final String ASSET_ID = "assetId";
  private static final String SOURCE_FOLDER_PATH = "sourceFolderPath";
  private static final String FILE_NAME = "fileName";
}