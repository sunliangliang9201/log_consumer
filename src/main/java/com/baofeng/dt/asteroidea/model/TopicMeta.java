package com.baofeng.dt.asteroidea.model;

/**
 * @author mignjia
 * @date 17/3/16
 */
public class TopicMeta {

    private Long id;

    private String topics;

    private String groupID;

    private int numThreads;

    private String fileType;

    private String codeC;

    private String filePath;

    private String filePathFormat = "yyyy-MM-dd";

    private String filePathMode;

    private String fileSuffix="";

    private int  isWriteLineBreaks;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTopics() {
        return topics;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }

    public String getCodeC() {
        return codeC;
    }

    public void setCodeC(String codeC) {
        this.codeC = codeC;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getFilePathFormat() {
        return filePathFormat;
    }

    public void setFilePathFormat(String filePathFormat) {
        this.filePathFormat = filePathFormat;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePathMode() {
        return filePathMode;
    }

    public void setFilePathMode(String filePathMode) {
        this.filePathMode = filePathMode;
    }

    public String getFileSuffix() {
        return fileSuffix;
    }

    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    public String getGroupID() {
        return groupID;
    }

    public void setGroupID(String groupID) {
        this.groupID = groupID;
    }

    public int getIsWriteLineBreaks() {
        return isWriteLineBreaks;
    }

    public void setIsWriteLineBreaks(int isWriteLineBreaks) {
        this.isWriteLineBreaks = isWriteLineBreaks;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    @Override
    public String toString() {
        return "TopicMeta{" +
                "id=" + id +
                ", topics='" + topics + '\'' +
                ", groupID='" + groupID + '\'' +
                ", numThreads=" + numThreads +
                ", fileType='" + fileType + '\'' +
                ", codeC='" + codeC + '\'' +
                ", filePath='" + filePath + '\'' +
                ", filePathFormat='" + filePathFormat + '\'' +
                ", filePathMode='" + filePathMode + '\'' +
                ", fileSuffix='" + fileSuffix + '\'' +
                ", isWriteLineBreaks=" + isWriteLineBreaks +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicMeta topicMeta = (TopicMeta) o;

        if (numThreads != topicMeta.numThreads) return false;
        if (isWriteLineBreaks != topicMeta.isWriteLineBreaks) return false;
        if (!id.equals(topicMeta.id)) return false;
        if (!topics.equals(topicMeta.topics)) return false;
        if (!groupID.equals(topicMeta.groupID)) return false;
        if (!fileType.equals(topicMeta.fileType)) return false;
        if (codeC != null ? !codeC.equals(topicMeta.codeC) : topicMeta.codeC != null) return false;
        if (!filePath.equals(topicMeta.filePath)) return false;
        if (!filePathFormat.equals(topicMeta.filePathFormat)) return false;
        if (!filePathMode.equals(topicMeta.filePathMode)) return false;
        return fileSuffix.equals(topicMeta.fileSuffix);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + topics.hashCode();
        result = 31 * result + groupID.hashCode();
        result = 31 * result + numThreads;
        result = 31 * result + fileType.hashCode();
        result = 31 * result + (codeC != null ? codeC.hashCode() : 0);
        result = 31 * result + filePath.hashCode();
        result = 31 * result + filePathFormat.hashCode();
        result = 31 * result + filePathMode.hashCode();
        result = 31 * result + fileSuffix.hashCode();
        result = 31 * result + isWriteLineBreaks;
        return result;
    }
}
