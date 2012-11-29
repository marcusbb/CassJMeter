package com.rim.icrs.cassjmeterext.gui;

import java.awt.GridBagConstraints;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.border.BevelBorder;

import org.apache.jmeter.testelement.TestElement;


import com.rim.icrs.cassjmeterext.gui.components.CompositeSerializerList;
import com.rim.icrs.cassjmeterext.sampler.EnhancedAbstractSampler;
import com.rim.icrs.cassjmeterext.sampler.EnhancedBatchPutSampler;
import com.rim.icrs.cassjmeterext.sampler.EnhancedPutSampler;

public class EnhancedBatchPut extends EnhancedAbstractGUI
{
    private static final long serialVersionUID = 3197090412869386190L;
    public static String LABEL = "Cassandra Enhanced Batch Put";
    private JTextArea NAME_VALUE_SEPARATOR;
    private JTextArea NAME_AND_VALUE;
    
    private JComboBox KSERIALIZER;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;
    private JCheckBox IS_COUNTER;
    
    private CompositeSerializerList COMPOSITE_KSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_CSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_VSERIALIZERS_LIST;
    
    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        NAME_AND_VALUE.setText(element.getPropertyAsString(EnhancedBatchPutSampler.NAME_AND_VALUE));
        NAME_VALUE_SEPARATOR.setText(element.getPropertyAsString(EnhancedBatchPutSampler.NAME_VALUE_SEPARATOR));
        
        KSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedPutSampler.KEY_SERIALIZER_TYPE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedBatchPutSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedBatchPutSampler.VALUE_SERIALIZER_TYPE));
        
        IS_COUNTER.setSelected(element.getPropertyAsBoolean(EnhancedBatchPutSampler.IS_COUNTER));
        
        COMPOSITE_KSERIALIZERS_LIST.setListModelElements(EnhancedPutSampler.getAsArray(element.getPropertyAsString(EnhancedPutSampler.COMPOSITE_KEY_SERIALIZER_TYPES), ","));
        COMPOSITE_CSERIALIZERS_LIST.setListModelElements(EnhancedPutSampler.getAsArray(element.getPropertyAsString(EnhancedPutSampler.COMPOSITE_COLUMN_SERIALIZER_TYPES), ","));
        COMPOSITE_VSERIALIZERS_LIST.setListModelElements(EnhancedPutSampler.getAsArray(element.getPropertyAsString(EnhancedPutSampler.COMPOSITE_VALUE_SERIALIZER_TYPES), ","));
    }

    public TestElement createTestElement()
    {
        EnhancedBatchPutSampler sampler = new EnhancedBatchPutSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");
        return sampler;
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);
        if (sampler instanceof EnhancedBatchPutSampler)
        {
            EnhancedBatchPutSampler gSampler = (EnhancedBatchPutSampler) sampler;
            
            gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            
            gSampler.setNameValue(NAME_AND_VALUE.getText());
            gSampler.setNameValueSeparator(NAME_VALUE_SEPARATOR.getText());
            gSampler.setCounter(IS_COUNTER.isSelected());
            
            if("CompositeSerializer".equals(gSampler.getKSerializerType())){
            	gSampler.setCompositeKSerializerTypes(COMPOSITE_KSERIALIZERS_LIST.getListModelAsArray());
            }

            if("CompositeSerializer".equals(gSampler.getCSerializerType())){
            	gSampler.setCompositeCSerializerTypes(COMPOSITE_CSERIALIZERS_LIST.getListModelAsArray());
            }
            
            if("CompositeSerializer".equals(gSampler.getVSerializerType())){
            	gSampler.setCompositeVSerializerTypes(COMPOSITE_VSERIALIZERS_LIST.getListModelAsArray());
            }
        }
    }

    public void initFields()
    {
    	String defaultSeparator = "@@";
    	NAME_VALUE_SEPARATOR.setText(defaultSeparator);
        NAME_AND_VALUE.setText("${__Random(1,1000)}:${__Random(1,1000)}".concat(defaultSeparator).concat("${__Random(1,1000)}:${__Random(1,1000)}\n")
        						.concat("${__Random(1,1000)}:${__Random(1,1000)}").concat(defaultSeparator).concat("${__Random(1,1000)}:${__Random(1,1000)}"));
        
        KSERIALIZER.setSelectedItem("Key Serializer");
        CSERIALIZER.setSelectedItem("Column Serializer");
        VSERIALIZER.setSelectedItem("Value Serializer");
        IS_COUNTER.setSelected(false);
    }

    public void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints)
    {   
    	addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("Column Value Separator: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, NAME_VALUE_SEPARATOR = new JTextArea());
        NAME_VALUE_SEPARATOR.setBorder(new BevelBorder(BevelBorder.LOWERED));
        
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("Column K/V(eg: Name@@Value): ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, NAME_AND_VALUE = new JTextArea());
        NAME_AND_VALUE.setRows(10);
        NAME_AND_VALUE.setBorder(new BevelBorder(BevelBorder.LOWERED));

        addToPanel(mainPanel, labelConstraints, 0, 5, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 5, KSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 6, COMPOSITE_KSERIALIZERS_LIST = new CompositeSerializerList());
        KSERIALIZER.addActionListener(COMPOSITE_KSERIALIZERS_LIST);

        addToPanel(mainPanel, labelConstraints, 0, 6, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 6, CSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 7, COMPOSITE_CSERIALIZERS_LIST = new CompositeSerializerList());
        CSERIALIZER.addActionListener(COMPOSITE_CSERIALIZERS_LIST);
        
        addToPanel(mainPanel, labelConstraints, 0, 8, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 8, VSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 9, COMPOSITE_VSERIALIZERS_LIST = new CompositeSerializerList());
        VSERIALIZER.addActionListener(COMPOSITE_VSERIALIZERS_LIST);
        
        addToPanel(mainPanel, labelConstraints, 0, 10, new JLabel("Counter: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 10, IS_COUNTER = new JCheckBox());
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }
}
