class XpathHelpers
  def self.isEqual?(v1, v2)
    v1.gsub(/\s+/, "") == v2.gsub(/\s+/, "")
  end

  def self.find_xpath(xpath, xml)
    doc = Nokogiri::XML(xml)
    doc.remove_namespaces!
    doc.xpath(xpath)
  end
  
  def self.getDIFEntryID(echo_xml)
    self.find_xpath("/Collection/AssociatedDIFs/DIF/EntryId", echo_xml).text
  end
end